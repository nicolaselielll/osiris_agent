import asyncio
import os
import random
import threading
import time
from collections import deque

import psutil
import rclpy
import websockets
import json

from rcl_interfaces.srv import GetParameters, ListParameters
from rclpy.node import Node
from rclpy.parameter import parameter_value_to_python
from rclpy.qos import QoSProfile
from rosidl_runtime_py import message_to_ordereddict
from rosidl_runtime_py.utilities import get_message

from osiris_agent import __version__ as AGENT_VERSION
from .bt_collector import BTCollector
from .ros2_control_collector import Ros2ControlCollector
from .tf_tree_collector import TfTreeCollector

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
GRAPH_CHECK_INTERVAL       = 2.0   # seconds between graph polls
TOPIC_BATCH_SIZE           = 10    # max topics enriched (deep-scan) per tick
TELEMETRY_INTERVAL         = 1.0   # seconds between telemetry samples
SERVICE_SCAN_INTERVAL      = 30.0  # seconds between service graph scans
PARAMETER_REFRESH_INTERVAL = 60.0  # seconds between retries for nodes with no params yet
MAX_SUBSCRIPTIONS      = 100   # hard cap on gateway-requested topic subs
RECONNECT_INITIAL_DELAY = 1    # seconds
RECONNECT_MAX_DELAY    = 30    # seconds

# Services to suppress from graph output (internal ROS2 plumbing)
_SUPPRESSED_SERVICE_PREFIXES = ('/ros2cli_daemon',)


class WebBridge(Node):

    # ──────────────────────────────────────────────
    # Init
    # ──────────────────────────────────────────────

    def __init__(self):
        super().__init__('osiris_node')

        # Auth token (required)
        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")

        # Declare tunable parameters
        self.declare_parameter('graph_check_interval',     GRAPH_CHECK_INTERVAL)
        self.declare_parameter('topic_batch_size',         TOPIC_BATCH_SIZE)
        self.declare_parameter('telemetry_interval',       TELEMETRY_INTERVAL)
        self.declare_parameter('tf_tree_enabled',          False)

        # WebSocket URL
        base_url = os.environ.get('OSIRIS_WS_URL', 'wss://osiris-gateway.fly.dev')
        # self.ws_url = f'{base_url}?robot=true&token={auth_token}'
        self.ws_url = f'ws://host.docker.internal:8080?robot=true&token={auth_token}'

        self.ws   = None
        self.loop = None
        self._send_queue: asyncio.Queue | None = None

        # ── Topic subscriptions (gateway-requested) ──────────────────────────
        self._topic_subs: dict[str, rclpy.subscription.Subscription] = {}
        self._topic_subs_lock = threading.Lock()
        self._topic_last_timestamp: dict[str, float] = {}
        self._topic_rate_history: dict[str, deque] = {}
        self._rate_history_depth = 8

        # ── Lifecycle subscriptions (auto-detected) ───────────────────────────
        self._lifecycle_subs: dict[str, rclpy.subscription.Subscription] = {}  # topic → sub
        self._lifecycle_state_cache: dict[str, str] = {}   # node_fqn → state label
        self._pending_lifecycle_fetches: set[str] = set()  # node_fqns with in-flight get_state calls

        # ── Existence caches (set of fully-qualified names) ───────────────────
        # Populated on first tick — NOT pre-populated here to avoid any DDS calls
        # during __init__ (which would contend Nav2's controller loop).
        self._active_nodes:    set[str] = set()   # fqn, e.g. /bt_navigator
        self._active_topics:   set[str] = set()   # e.g. /cmd_vel
        self._active_services: dict[str, str] = {}  # service_name → type_str
        self._active_actions:  set[str] = set()   # action name (no /_action/status suffix)

        # ── Count sentinels (cheap change detection) ─────────────────────────
        # count_publishers/count_subscribers are O(1) hash-map lookups in DDS.
        # When a count changes, the topic goes into the enrichment pending set.
        self._topic_counts: dict[str, tuple[int, int]] = {}  # topic → (pub_n, sub_n)

        # ── Relation caches (populated by Tier-2 enrichment) ─────────────────
        self._topic_relations: dict[str, dict] = {}
        # {topic: {publishers: set[fqn], subscribers: set[fqn],
        #          publisher_infos: list, subscriber_infos: list, type: str}}

        # Service relations are type-only (no expensive per-service provider scan)
        # _active_services already holds {name: type}

        # Action relations derived from _topic_relations at snapshot time
        # (zero extra DDS calls)

        # ── Enrichment pending queues ─────────────────────────────────────────
        self._pending_topic_enrichment: set[str] = set()

        # ── Parameters (lazy-loaded, async) ──────────────────────────────────
        self._node_parameter_cache: dict[str, dict] = {}
        self._pending_param_fetches: set[str] = set()



        # ── Snapshot & dirty-flag ─────────────────────────────────────────────
        self._last_sent_nodes:    dict | None = None
        self._last_sent_topics:   dict | None = None
        self._last_sent_actions:  dict | None = None
        self._last_sent_services: dict | None = None
        self._graph_dirty = False

        # ── Service scan throttle ──────────────────────────────────────────────
        # get_service_names_and_types() is expensive with hundreds of services;
        # only scan on first tick and every SERVICE_SCAN_INTERVAL seconds after.
        # _service_rescan_ticks forces extra scans after node loss (DDS lag).
        self._last_service_scan: float = 0.0
        self._service_rescan_ticks: int = 0

        # ── Initial scan synchronization ──────────────────────────────────────
        # _initial_scan_complete is set on the first _check_graph_changes tick
        # so _send_initial_state waits until caches are fully populated.
        self._initial_scan_complete = threading.Event()
        self._first_graph_check_done = False

        # ── BT state ─────────────────────────────────────────────────────────
        self._cached_bt_tree_event: dict | None = None

        # ── Telemetry ─────────────────────────────────────────────────────────
        self._telemetry_enabled = True
        self._last_disk_io      = None
        self._last_net_io       = None
        self._last_io_time:     float | None = None
        self._cpu_history:      deque = deque(maxlen=900)  # 15 min at 1 Hz
        psutil.cpu_percent(interval=None)  # prime — first call always returns 0.0

        # ── Collectors ────────────────────────────────────────────────────────
        self._ros2_control = Ros2ControlCollector(
            node=self,
            event_callback=self._on_ros2_control_event,
            logger=self.get_logger(),
        )
        _tf_tree_enabled = self.get_parameter('tf_tree_enabled').get_parameter_value().bool_value
        self._tf_tree = TfTreeCollector(
            node=self,
            event_callback=self._on_tf_tree_event,
            logger=self.get_logger(),
        ) if _tf_tree_enabled else None

        # ── Timers ───────────────────────────────────────────────────────────
        _graph_interval = self.get_parameter('graph_check_interval').get_parameter_value().double_value
        _telemetry_interval = self.get_parameter('telemetry_interval').get_parameter_value().double_value

        self.create_timer(_graph_interval,          self._check_graph_changes)
        self.create_timer(_telemetry_interval,       self._collect_telemetry)
        self.create_timer(PARAMETER_REFRESH_INTERVAL, self._refresh_empty_param_caches)
        self.create_timer(5.0,                        self._retry_lifecycle_state_fetches)
        self.create_timer(2.0,                        self._periodic_flush_graph)

        self._topic_batch_size = self.get_parameter('topic_batch_size').get_parameter_value().integer_value

        # ── WebSocket thread ──────────────────────────────────────────────────
        threading.Thread(target=self._run_ws_client, daemon=True).start()

        # ── Optional BT collectors ────────────────────────────────────────────
        bt_enabled = os.environ.get(
            'OSIRIS_BT_COLLECTOR_ENABLED', ''
        ).lower() in ('true', '1', 'yes')
        if bt_enabled:
            self._bt_collector = BTCollector(
                event_callback=self._on_bt_event,
                logger=self.get_logger(),
            )
            self._bt_collector.start()
        else:
            self._bt_collector = None

        self._init_nav2_bt_monitor()

        self.get_logger().info(
            f"🚀 Osiris agent v{AGENT_VERSION} — graph_interval={_graph_interval}s, "
            f"topic_batch={self._topic_batch_size}"
        )

    # ──────────────────────────────────────────────
    # WebSocket client
    # ──────────────────────────────────────────────

    def _run_ws_client(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._send_queue = asyncio.Queue()
        self.loop.run_until_complete(self._client_loop_with_reconnect())

    async def _client_loop_with_reconnect(self):
        delay = RECONNECT_INITIAL_DELAY
        while self.context.ok():
            try:
                await self._client_loop()
            except Exception as e:
                if self.context.ok():
                    self.get_logger().warning(
                        f"WebSocket error: {e}; retrying in {delay:.1f}s"
                    )
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY) + random.uniform(0, 1)

    async def _client_loop(self):
        send_task = None
        self.get_logger().info('Connecting to gateway...')
        try:
            async with websockets.connect(self.ws_url) as ws:
                try:
                    auth_msg = await ws.recv()
                    auth_data = json.loads(auth_msg)
                except Exception:
                    self.get_logger().error('Failed to receive auth response from gateway')
                    return

                if not auth_data or auth_data.get('type') != 'auth_success':
                    error_msg = auth_data.get('message', 'unknown') if auth_data else 'no response'
                    self.get_logger().error(f'Authentication failed: {error_msg}')
                    return

                self.get_logger().info('Connected and authenticated to gateway')
                self.ws = ws
                send_task = asyncio.create_task(self._send_loop(ws))

                await self._send_initial_state()
                await self._receive_loop(ws)
        finally:
            if send_task and not send_task.done():
                send_task.cancel()
                try:
                    await send_task
                except (asyncio.CancelledError, Exception):
                    pass
            if self.ws is not None:
                self.get_logger().warning('Disconnected from gateway')
            self.ws = None

    async def _send_loop(self, ws):
        while True:
            msg = await self._send_queue.get()
            try:
                await ws.send(msg)
            except Exception as e:
                self.get_logger().error(f"WS send failed: {e}")
                raise

    async def _receive_loop(self, ws):
        async for raw in ws:
            if not self.context.ok():
                break
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            msg_type = data.get('type')
            if msg_type == 'subscribe':
                topic = data.get('topic')
                if topic:
                    self._subscribe_to_topic(topic)
            elif msg_type == 'unsubscribe':
                topic = data.get('topic')
                if topic:
                    self._unsubscribe_from_topic(topic)
            elif msg_type == 'start_telemetry':
                self._telemetry_enabled = True
            elif msg_type == 'stop_telemetry':
                self._telemetry_enabled = False
            elif msg_type == 'error':
                self.get_logger().warning(f"Gateway error: {data.get('message', '')}")

    # ──────────────────────────────────────────────
    # Initial state
    # ──────────────────────────────────────────────

    async def _send_initial_state(self):
        # Wait for the first _check_graph_changes tick to populate all caches.
        # Times out after 15s as a safety net in case the executor is slow to start.
        await asyncio.to_thread(self._initial_scan_complete.wait, 15.0)

        # Reset delta caches so _flush_graph_snapshots treats everything as
        # "unsent" after this reconnect — any change that occurred while
        # the WS was down will be immediately re-broadcast.
        self._last_sent_nodes    = None
        self._last_sent_topics   = None
        self._last_sent_actions  = None
        self._last_sent_services = None
        self._graph_dirty        = True

        nodes    = self._get_nodes_with_relations()
        topics   = self._get_topics_with_relations()
        actions  = self._get_actions_with_relations()
        services = self._get_services_with_relations()

        self._last_sent_nodes    = nodes.copy()
        self._last_sent_topics   = topics.copy()
        self._last_sent_actions  = actions.copy()
        self._last_sent_services = services.copy()

        await self._send_queue.put(json.dumps({
            'type': 'agent_version',
            'version': AGENT_VERSION,
        }))

        await self._send_queue.put(json.dumps({
            'type': 'initial_state',
            'timestamp': time.time(),
            'data': {
                'nodes':       nodes,
                'topics':      topics,
                'actions':     actions,
                'services':    services,
                'telemetry':   self._get_telemetry_snapshot(),
                'controllers': self._ros2_control.get_controllers_snapshot(),
                'hardware':    self._ros2_control.get_hardware_snapshot(),
                'tf_tree':     self._tf_tree.get_snapshot() if self._tf_tree is not None else None,
            },
        }))

        await self._send_queue.put(json.dumps(self._build_startup_bt_state_event()))

        if self._cached_bt_tree_event:
            await self._send_queue.put(json.dumps(self._cached_bt_tree_event))
            self._cached_bt_tree_event = None

        await self._send_bridge_subscriptions()

        self.get_logger().info(
            f"Sent initial_state: {len(nodes)} nodes, {len(topics)} topics, "
            f"{len(actions)} actions, {len(services)} services"
        )

    async def _send_bridge_subscriptions(self):
        with self._topic_subs_lock:
            subs = list(self._topic_subs.keys())
        await self._send_queue.put(json.dumps({
            'type': 'bridge_subscriptions',
            'subscriptions': subs,
            'timestamp': time.time(),
        }))

    # ──────────────────────────────────────────────
    # Tier-1: cheap existence detection
    # ──────────────────────────────────────────────

    def _check_graph_changes(self):
        if self._first_graph_check_done:
            return  # BISECT R1: skip all polling after first tick

        # ── 1. Node + topic queries (always, both cheap) ──────────────────────
        _t0 = time.time()
        node_pairs      = list(self.get_node_names_and_namespaces())
        topic_type_list = self.get_topic_names_and_types()
        _t1 = time.time()

        current_nodes   = {self._node_full_name(n, ns) for n, ns in node_pairs}
        current_topics  = {t for t, _ in topic_type_list}
        current_actions = {
            t.replace('/_action/status', '')
            for t in current_topics
            if t.endswith('/_action/status')
        }
        self.get_logger().info(
            f"[poll] node+topic: {(_t1-_t0)*1000:.1f}ms "
            f"({len(current_nodes)} nodes, {len(current_topics)} topics, {len(current_actions)} actions)"
        )

        # ── 1b. Service scan — throttled to SERVICE_SCAN_INTERVAL ─────────────
        # get_service_names_and_types() enumerates every service in DDS and is
        # expensive on large stacks (500+ services). Also force a rescan any time
        # a node disappears so stale services are evicted immediately.
        _now = time.time()
        _nodes_stopped = self._first_graph_check_done and bool(self._active_nodes - current_nodes)
        _do_service_scan = (
            not self._first_graph_check_done
            or _nodes_stopped
            or self._service_rescan_ticks > 0
            or (_now - self._last_service_scan) >= SERVICE_SCAN_INTERVAL
        )
        if _do_service_scan:
            self._last_service_scan = _now
            if _nodes_stopped:
                # Schedule follow-up scans to catch DDS endpoint lag.
                self._service_rescan_ticks = 4
            elif self._service_rescan_ticks > 0:
                self._service_rescan_ticks -= 1
            _ts0 = time.time()
            service_type_list = self.get_service_names_and_types()
            _ts1 = time.time()
            current_services = {
                s: types[0] if types else 'unknown'
                for s, types in service_type_list
                if not any(s.startswith(p) for p in _SUPPRESSED_SERVICE_PREFIXES)
            }
            self.get_logger().info(
                f"[poll] service_scan: {(_ts1-_ts0)*1000:.1f}ms ({len(current_services)} services)"
            )
        else:
            current_services = self._active_services

        # ── FIRST TICK: silently populate caches, no events ───────────────────
        # The WS thread is waiting on _initial_scan_complete before sending
        # initial_state. Populate everything here so the snapshot is full.
        # Subsequent ticks will only emit events for actual changes.
        if not self._first_graph_check_done:
            self._first_graph_check_done = True
            self._active_nodes    = current_nodes
            self._active_topics   = current_topics
            self._active_services = current_services
            self._active_actions  = current_actions
            _te0 = time.time()
            self._do_full_initial_enrichment(topic_type_list, node_pairs)
            _te1 = time.time()
            for fqn in current_nodes:
                self._fetch_node_parameters_async(fqn)
            for t in current_topics:
                if t.endswith('/transition_event'):
                    self._subscribe_lifecycle_topic(t)
                    node_fqn = t[:-len('/transition_event')]
                    self._fetch_lifecycle_state_async(node_fqn)
            self._ros2_control.poll()
            if self._tf_tree is not None:
                self._tf_tree.poll(force=True)
            self._initial_scan_complete.set()
            self.get_logger().info(
                f"[poll] first tick complete: {len(current_nodes)} nodes, {len(current_topics)} topics, "
                f"{len(current_services)} services, {len(current_actions)} actions — "
                f"node+topic={(_t1-_t0)*1000:.1f}ms enrichment={(_te1-_te0)*1000:.1f}ms"
            )
            return

        # ── 2. Node events ────────────────────────────────────────────────────
        started_nodes = current_nodes - self._active_nodes
        if started_nodes:
            self.get_logger().info(f"[poll] {len(started_nodes)} node(s) started: {sorted(started_nodes)}")
            # New nodes may subscribe to existing topics — queue all known topics
            # for re-enrichment so their memberships are reflected accurately.
            self._pending_topic_enrichment.update(self._active_topics)
        for fqn in started_nodes:
            self._fetch_node_parameters_async(fqn)
            self._send_event_and_update({
                'type': 'node_event', 'node': fqn,
                'event': 'started', 'timestamp': time.time(),
            })

        stopped_nodes = self._active_nodes - current_nodes
        if stopped_nodes:
            self.get_logger().info(f"[poll] {len(stopped_nodes)} node(s) stopped: {sorted(stopped_nodes)}")
        for fqn in stopped_nodes:
            # Re-enrich every topic this node was involved in so stale
            # publisher/subscriber entries are removed from the relations cache.
            for topic, rel in self._topic_relations.items():
                if fqn in rel.get('publishers', set()) or fqn in rel.get('subscribers', set()):
                    self._pending_topic_enrichment.add(topic)
            # Drop stale parameter data for the stopped node.
            self._node_parameter_cache.pop(fqn, None)
            self._pending_param_fetches.discard(fqn)
            self._send_event_and_update({
                'type': 'node_event', 'node': fqn,
                'event': 'stopped', 'timestamp': time.time(),
            })

        # ── 3. Topic events ───────────────────────────────────────────────────
        for t in current_topics - self._active_topics:
            self._pending_topic_enrichment.add(t)
            self._send_event_and_update({
                'type': 'topic_event', 'topic': t,
                'event': 'created', 'timestamp': time.time(),
            })
            if t.endswith('/transition_event'):
                self._subscribe_lifecycle_topic(t)
            # Nav2 BT edge-case: /behavior_tree_log just appeared
            if t == '/behavior_tree_log' and hasattr(self, '_nav2_bt_tree_id'):
                if self.count_publishers(t) > 0:
                    self._nav2_bt_publisher_active = True
                    if self._load_and_parse_bt_xml():
                        self._on_bt_event({
                            'type': 'bt_tree', 'timestamp': time.time(),
                            'tree_id': self._nav2_bt_tree_id,
                            'tree': self._nav2_bt_tree_structure,
                            'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                        })

        for t in self._active_topics - current_topics:
            self._topic_relations.pop(t, None)
            self._topic_counts.pop(t, None)
            self._pending_topic_enrichment.discard(t)
            self._send_event_and_update({
                'type': 'topic_event', 'topic': t,
                'event': 'destroyed', 'timestamp': time.time(),
            })
            if t.endswith('/transition_event'):
                lc_sub = self._lifecycle_subs.pop(t, None)
                if lc_sub:
                    self.destroy_subscription(lc_sub)
            if t == '/behavior_tree_log' and hasattr(self, '_nav2_bt_tree_id'):
                self._on_nav2_bt_gone()

        # ── 4. Service events (only every SERVICE_SCAN_INTERVAL) ──────────────
        if _do_service_scan:
            for s in set(current_services) - set(self._active_services):
                self._send_event_and_update({
                    'type': 'service_event', 'service': s,
                    'event': 'created', 'timestamp': time.time(),
                })

            for s in set(self._active_services) - set(current_services):
                self._send_event_and_update({
                    'type': 'service_event', 'service': s,
                    'event': 'destroyed', 'timestamp': time.time(),
                })

        # ── 5. Action events ──────────────────────────────────────────────────
        for a in current_actions - self._active_actions:
            self._send_event_and_update({
                'type': 'action_event', 'action': a,
                'event': 'created', 'timestamp': time.time(),
            })

        for a in self._active_actions - current_actions:
            self._send_event_and_update({
                'type': 'action_event', 'action': a,
                'event': 'destroyed', 'timestamp': time.time(),
            })

        # ── 6. Update existence caches ────────────────────────────────────────
        self._active_nodes    = current_nodes
        self._active_topics   = current_topics
        if _do_service_scan:
            self._active_services = current_services
        self._active_actions  = current_actions

        # ── 7. Tier-2: batched relation enrichment ────────────────────────────
        self._enrich_pending_relations(topic_type_list)

        # ── 9. Nav2 BT publisher liveness check (uses cached topic relations) ─
        if hasattr(self, '_nav2_bt_publisher_active'):
            bt_rel = self._topic_relations.get('/behavior_tree_log', {})
            bt_pubs = bt_rel.get('publishers', set()) & current_nodes
            if self._nav2_bt_publisher_active and not bt_pubs:
                self._on_nav2_bt_gone()
            elif self._nav2_bt_publisher_active and bt_pubs and self._nav2_bt_tree_id is None:
                # Publisher is live but we failed to load the BT XML at startup
                # (executor wasn't spinning yet) — retry every tick until it works.
                if self._load_and_parse_bt_xml():
                    self._on_bt_event({
                        'type': 'bt_tree', 'timestamp': time.time(),
                        'tree_id': self._nav2_bt_tree_id,
                        'tree': self._nav2_bt_tree_structure,
                        'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                    })
            elif not self._nav2_bt_publisher_active and bt_pubs:
                self._nav2_bt_publisher_active = True
                if self._load_and_parse_bt_xml():
                    self._on_bt_event({
                        'type': 'bt_tree', 'timestamp': time.time(),
                        'tree_id': self._nav2_bt_tree_id,
                        'tree': self._nav2_bt_tree_structure,
                        'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                    })

        # ── 10. Flush graph snapshots if anything changed ─────────────────────
        self._flush_graph_snapshots()

        # ── 11. Poll collectors (internally rate-limited) ─────────────────────
        self._ros2_control.poll()
        if self._tf_tree is not None:
            self._tf_tree.poll()

    # ──────────────────────────────────────────────
    # Initial full enrichment (called once on first tick)
    # ──────────────────────────────────────────────

    def _do_full_initial_enrichment(self, topic_type_list, node_pairs):
        """Enrich every known topic synchronously for the initial state snapshot.

        Called once on the first _check_graph_changes tick (before any WS events
        are emitted). Runs unbatched so initial_state contains full relation data.
        Subsequent changes are handled by the normal batched Tier-2 path.
        """
        topic_type_map = dict(topic_type_list)
        self._pending_topic_enrichment.clear()

        for topic in self._active_topics:
            try:
                pub_infos = self.get_publishers_info_by_topic(topic)
                sub_infos = self.get_subscriptions_info_by_topic(topic)
            except Exception:
                continue
            publishers = {self._node_full_name(p.node_name, p.node_namespace) for p in pub_infos}
            subscribers = {self._node_full_name(s.node_name, s.node_namespace) for s in sub_infos}
            self._topic_relations[topic] = {
                'publishers':       publishers,
                'subscribers':      subscribers,
                'publisher_infos':  pub_infos,
                'subscriber_infos': sub_infos,
                'type': topic_type_map.get(topic, ['unknown'])[0],
            }
            self._topic_counts[topic] = (len(pub_infos), len(sub_infos))

    # ──────────────────────────────────────────────
    # Tier-2: batched relation enrichment
    # ──────────────────────────────────────────────

    def _enrich_pending_relations(self, topic_type_list=None):
        """Process up to TOPIC_BATCH_SIZE topics from the pending enrichment set.

        Uses get_publishers_info_by_topic() and get_subscriptions_info_by_topic()
        which allocate memory and hold the DDS participant mutex briefly.
        Batching ensures we never block the executor for more than ~16–32 ms
        even during a burst (e.g. Nav2 startup creating 40 topics at once).
        """
        if not self._pending_topic_enrichment:
            return

        _pending_before = len(self._pending_topic_enrichment)
        _t0 = time.time()
        batch = set(list(self._pending_topic_enrichment)[:self._topic_batch_size])
        self._pending_topic_enrichment -= batch
        self.get_logger().info(
            f"[enrich] batch={len(batch)}, pending_before={_pending_before}, "
            f"remaining={len(self._pending_topic_enrichment)}"
        )

        # Reuse the already-fetched type list from Tier-1 when available,
        # avoiding a redundant global DDS query.
        if topic_type_list is not None:
            topic_type_map = dict(topic_type_list)
        else:
            topic_type_map = dict(self.get_topic_names_and_types())

        for topic in batch:
            if topic not in self._active_topics:
                continue  # topic disappeared between Tier-1 and now
            try:
                pub_infos = self.get_publishers_info_by_topic(topic)
                sub_infos = self.get_subscriptions_info_by_topic(topic)
            except Exception as e:
                self.get_logger().debug(f"Enrichment failed for {topic}: {e}")
                continue

            publishers = {self._node_full_name(p.node_name, p.node_namespace) for p in pub_infos}
            subscribers = {self._node_full_name(s.node_name, s.node_namespace) for s in sub_infos}

            old = self._topic_relations.get(topic)
            new_rel = {
                'publishers':      publishers,
                'subscribers':     subscribers,
                'publisher_infos': pub_infos,
                'subscriber_infos': sub_infos,
                'type': topic_type_map.get(topic, ['unknown'])[0],
            }
            self._topic_relations[topic] = new_rel
            # Update count sentinel from Tier-2 results for free — avoids a
            # redundant count_publishers/count_subscribers call next tick.
            self._topic_counts[topic] = (len(pub_infos), len(sub_infos))

            # Emit subscriber-joined / subscriber-left events
            if old is not None:
                old_subs = old['subscribers']
                for fqn in subscribers - old_subs:
                    self._send_event_and_update({
                        'type': 'topic_event', 'topic': topic, 'node': fqn,
                        'event': 'subscribed', 'timestamp': time.time(),
                    })
                for fqn in old_subs - subscribers:
                    self._send_event_and_update({
                        'type': 'topic_event', 'topic': topic, 'node': fqn,
                        'event': 'unsubscribed', 'timestamp': time.time(),
                    })

                # Nav2 BT: publisher appeared/vanished on /behavior_tree_log
                if topic == '/behavior_tree_log' and hasattr(self, '_nav2_bt_tree_id'):
                    old_pubs = old['publishers']
                    if publishers and not old_pubs:
                        self._nav2_bt_publisher_active = True
                        if self._load_and_parse_bt_xml():
                            self._on_bt_event({
                                'type': 'bt_tree', 'timestamp': time.time(),
                                'tree_id': self._nav2_bt_tree_id,
                                'tree': self._nav2_bt_tree_structure,
                                'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                            })
                    elif old_pubs and not publishers:
                        self._on_nav2_bt_gone()

        self.get_logger().info(f"[enrich] done in {(time.time()-_t0)*1000:.1f}ms")

    # ──────────────────────────────────────────────
    # Graph snapshot builders
    # ──────────────────────────────────────────────

    def _get_nodes_with_relations(self) -> dict:
        result = {}
        for fqn in self._active_nodes:
            result[fqn] = {
                'publishes':  [],
                'subscribes': [],
                'actions':    [],
                'services':   [],
                'parameters': self._node_parameter_cache.get(fqn, {}),
                'lifecycle_state': self._lifecycle_state_cache.get(fqn, None),
            }

        # Derive pub/sub per node from cached topic relations (zero DDS calls)
        for topic, rel in self._topic_relations.items():
            pub_infos = rel.get('publisher_infos', [])
            sub_infos = rel.get('subscriber_infos', [])
            for p in pub_infos:
                fqn = self._node_full_name(p.node_name, p.node_namespace)
                if fqn in result:
                    result[fqn]['publishes'].append({
                        'topic': topic,
                        'qos': self._qos_to_dict(p.qos_profile),
                    })
            for s in sub_infos:
                fqn = self._node_full_name(s.node_name, s.node_namespace)
                if fqn in result:
                    result[fqn]['subscribes'].append({
                        'topic': topic,
                        'qos': self._qos_to_dict(s.qos_profile),
                    })

        # Actions: derived from topic relations (zero DDS calls)
        for topic, rel in self._topic_relations.items():
            if topic.endswith('/_action/status') and rel['publishers']:
                action = topic.replace('/_action/status', '')
                for p in rel['publisher_infos']:
                    fqn = self._node_full_name(p.node_name, p.node_namespace)
                    if fqn in result and action not in result[fqn]['actions']:
                        result[fqn]['actions'].append(action)

        return result

    def _get_topics_with_relations(self) -> dict:
        result = {}
        for topic, rel in self._topic_relations.items():
            result[topic] = {
                'type': rel.get('type', 'unknown'),
                'publishers': [
                    {
                        'node': self._node_full_name(p.node_name, p.node_namespace),
                        'qos': self._qos_to_dict(p.qos_profile),
                    }
                    for p in rel.get('publisher_infos', [])
                ],
                'subscribers': [
                    {
                        'node': self._node_full_name(s.node_name, s.node_namespace),
                        'qos': self._qos_to_dict(s.qos_profile),
                    }
                    for s in rel.get('subscriber_infos', [])
                ],
            }
        return result

    def _get_actions_with_relations(self) -> dict:
        result = {}
        for topic, rel in self._topic_relations.items():
            if topic.endswith('/_action/status') and rel['publishers']:
                action = topic.replace('/_action/status', '')
                providers = [
                    self._node_full_name(p.node_name, p.node_namespace)
                    for p in rel.get('publisher_infos', [])
                ]
                result[action] = {'providers': providers}
        return result

    def _get_services_with_relations(self) -> dict:
        return {
            name: {'type': type_str, 'providers': []}
            for name, type_str in self._active_services.items()
        }

    # ──────────────────────────────────────────────
    # Delta-send: flush graph snapshots after each tick
    # ──────────────────────────────────────────────

    def _flush_graph_snapshots(self):
        if not self._graph_dirty or not self.ws or not self.loop:
            return
        self._graph_dirty = False
        self.get_logger().debug("[flush] graph dirty, checking snapshots")

        nodes = self._get_nodes_with_relations()
        if nodes != self._last_sent_nodes:
            self.get_logger().info(f"[flush] nodes changed ({len(nodes)} nodes)")
            self._last_sent_nodes = nodes.copy()
            self._enqueue({
                'type': 'nodes', 'data': nodes, 'timestamp': time.time(),
            })

        topics = self._get_topics_with_relations()
        if topics != self._last_sent_topics:
            self.get_logger().info(f"[flush] topics changed ({len(topics)} topics)")
            self._last_sent_topics = topics.copy()
            self._enqueue({
                'type': 'topics', 'data': topics, 'timestamp': time.time(),
            })

        actions = self._get_actions_with_relations()
        if actions != self._last_sent_actions:
            self.get_logger().info(f"[flush] actions changed ({len(actions)} actions)")
            self._last_sent_actions = actions.copy()
            self._enqueue({
                'type': 'actions', 'data': actions, 'timestamp': time.time(),
            })

        services = self._get_services_with_relations()
        if services != self._last_sent_services:
            self.get_logger().info(f"[flush] services changed ({len(services)} services)")
            self._last_sent_services = services.copy()
            self._enqueue({
                'type': 'services', 'data': services, 'timestamp': time.time(),
            })

    # ──────────────────────────────────────────────
    # Topic subscriptions (gateway-requested)
    # ──────────────────────────────────────────────

    def _subscribe_to_topic(self, topic_name: str):
        if not topic_name or not isinstance(topic_name, str):
            return
        with self._topic_subs_lock:
            if topic_name in self._topic_subs:
                return
            if len(self._topic_subs) >= MAX_SUBSCRIPTIONS:
                self.get_logger().error(
                    f"Subscription limit ({MAX_SUBSCRIPTIONS}) reached; "
                    f"cannot subscribe to {topic_name}"
                )
                return

        types = dict(self.get_topic_names_and_types()).get(topic_name)
        if not types:
            self.get_logger().warning(f"Topic not found: {topic_name}")
            return

        msg_class = get_message(types[0])
        sub = self.create_subscription(
            msg_class,
            topic_name,
            lambda msg, t=topic_name: self._on_topic_msg(msg, t),
            QoSProfile(depth=10),
        )
        with self._topic_subs_lock:
            self._topic_subs[topic_name] = sub

        self.get_logger().info(f"Subscribed to {topic_name}")
        if self.loop:
            asyncio.run_coroutine_threadsafe(
                self._send_bridge_subscriptions(), self.loop
            )

    def _unsubscribe_from_topic(self, topic_name: str):
        with self._topic_subs_lock:
            sub = self._topic_subs.pop(topic_name, None)
        if sub:
            self.destroy_subscription(sub)
            self.get_logger().info(f"Unsubscribed from {topic_name}")
            if self.loop:
                asyncio.run_coroutine_threadsafe(
                    self._send_bridge_subscriptions(), self.loop
                )

    # ──────────────────────────────────────────────
    # Lifecycle subscriptions (auto-detected managed nodes)
    # ──────────────────────────────────────────────

    def _subscribe_lifecycle_topic(self, topic: str):
        """Subscribe to a /<node>/transition_event topic for a lifecycle-managed node."""
        if topic in self._lifecycle_subs:
            return
        try:
            from lifecycle_msgs.msg import TransitionEvent
            node_fqn = topic[:-len('/transition_event')]
            sub = self.create_subscription(
                TransitionEvent,
                topic,
                lambda msg, n=node_fqn: self._on_lifecycle_transition(msg, n),
                QoSProfile(depth=10),
            )
            self._lifecycle_subs[topic] = sub
            self.get_logger().info(f'[lifecycle] subscribed to {topic}')
        except Exception as e:
            self.get_logger().debug(f'[lifecycle] could not subscribe to {topic}: {e}')

    def _periodic_flush_graph(self):
        """Periodically flush batched graph changes (lifecycle state, params, etc)."""
        self._flush_graph_snapshots()

    def _retry_lifecycle_state_fetches(self):
        """Retry get_state queries for lifecycle nodes that haven't responded yet."""
        if not self._first_graph_check_done:
            return
        for topic in list(self._lifecycle_subs):
            node_fqn = topic[:-len('/transition_event')]
            if node_fqn not in self._lifecycle_state_cache and node_fqn not in self._pending_lifecycle_fetches:
                self._fetch_lifecycle_state_async(node_fqn)

    def _fetch_lifecycle_state_async(self, node_fqn: str):
        """Query /<node>/get_state service to populate lifecycle_state_cache."""
        if node_fqn in self._lifecycle_state_cache:
            return  # already known (populated by transition event or previous fetch)
        if node_fqn in self._pending_lifecycle_fetches:
            return  # call already in flight
        try:
            from lifecycle_msgs.srv import GetState
        except ImportError:
            return
        client = self.create_client(GetState, f'{node_fqn}/get_state')
        if not client.service_is_ready():
            self.destroy_client(client)
            return  # _retry_lifecycle_state_fetches will retry at next 5s tick
        self._pending_lifecycle_fetches.add(node_fqn)
        future = client.call_async(GetState.Request())

        def _on_get_state(fut):
            self._pending_lifecycle_fetches.discard(node_fqn)
            self.destroy_client(client)
            try:
                resp = fut.result()
                if resp is not None:
                    self._lifecycle_state_cache[node_fqn] = resp.current_state.label
                    self._graph_dirty = True  # Batched flush via _periodic_flush_graph timer
                    self.get_logger().debug(
                        f'[lifecycle] initial state for {node_fqn}: {resp.current_state.label}'
                    )
            except Exception as e:
                self.get_logger().debug(f'[lifecycle] get_state failed for {node_fqn}: {e}')

        future.add_done_callback(_on_get_state)

    def _on_lifecycle_transition(self, msg, node_fqn: str):
        self._lifecycle_state_cache[node_fqn] = msg.goal_state.label
        self.get_logger().info(
            f'[lifecycle] {node_fqn}: {msg.start_state.label} \u2192 {msg.goal_state.label} '
            f'(transition: {msg.transition.label})'
        )
        self._send_event_and_update({
            'type': 'lifecycle_event',
            'node': node_fqn,
            'transition': msg.transition.label,
            'from_state': msg.start_state.label,
            'to_state': msg.goal_state.label,
            'timestamp': time.time(),
        })

    def _on_topic_msg(self, msg, topic_name: str):
        if not self.ws or not self.loop:
            return

        ts = time.time()
        last_ts = self._topic_last_timestamp.get(topic_name)
        if last_ts is not None:
            delta = ts - last_ts
            if delta > 0:
                history = self._topic_rate_history.setdefault(
                    topic_name, deque(maxlen=self._rate_history_depth)
                )
                history.append(delta)
        self._topic_last_timestamp[topic_name] = ts

        rate = None
        history = self._topic_rate_history.get(topic_name)
        if history:
            total = sum(history)
            if total > 0:
                rate = len(history) / total

        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps({
                'type': 'topic_data',
                'topic': topic_name,
                'data': message_to_ordereddict(msg),
                'rate_hz': rate,
                'timestamp': ts,
            })),
            self.loop,
        )

    # ──────────────────────────────────────────────
    # Telemetry
    # ──────────────────────────────────────────────

    # ──────────────────────────────────────────────
    # Parameters (async, lazy-loaded)
    # ──────────────────────────────────────────────

    def _refresh_empty_param_caches(self):
        """Retry parameter fetch for nodes that don't have cached params yet."""
        for fqn in self._active_nodes:
            if not self._node_parameter_cache.get(fqn):
                self._fetch_node_parameters_async(fqn)

    def _fetch_node_parameters_async(self, fqn: str):
        """Fetch parameters for *fqn* without blocking the executor.

        Creates service clients, fires async calls, and stores results in
        _node_parameter_cache when callbacks fire.  Safe to call from any
        timer or graph-change callback.
        """
        if fqn in self._pending_param_fetches:
            return

        list_client = self.create_client(ListParameters, f"{fqn}/list_parameters")
        if not list_client.service_is_ready():
            self.destroy_client(list_client)
            return

        self._pending_param_fetches.add(fqn)
        req = ListParameters.Request()
        req.depth = 10
        future = list_client.call_async(req)

        def _on_list(fut):
            self.destroy_client(list_client)
            response = fut.result()
            if response is None or not response.result.names:
                self._pending_param_fetches.discard(fqn)
                return
            param_names = list(response.result.names)
            get_client = self.create_client(GetParameters, f"{fqn}/get_parameters")
            get_req = GetParameters.Request()
            get_req.names = param_names
            get_future = get_client.call_async(get_req)

            def _on_get(gfut):
                self.destroy_client(get_client)
                self._pending_param_fetches.discard(fqn)
                get_resp = gfut.result()
                if get_resp is None:
                    return
                params = {}
                for name, value in zip(param_names, get_resp.values):
                    try:
                        params[name] = parameter_value_to_python(value)
                    except Exception:
                        pass
                self._node_parameter_cache[fqn] = params
                self._graph_dirty = True
                self.get_logger().debug(f"[params] cached {len(params)} params for {fqn}")

            get_future.add_done_callback(_on_get)

        future.add_done_callback(_on_list)

    def _collect_telemetry(self):
        return  # BISECT R4a: disable telemetry
        if not self._telemetry_enabled or not self.ws or not self.loop:
            return
        self._enqueue({
            'type': 'telemetry',
            'data': self._get_telemetry_snapshot(),
            'timestamp': time.time(),
        })

    def _get_telemetry_snapshot(self) -> dict:
        # ── CPU: instantaneous + rolling averages ─────────────────────────────
        cpu_now = round(psutil.cpu_percent(interval=None), 1)
        self._cpu_history.append(cpu_now)

        def _rolling(n: int) -> float | None:
            window = list(self._cpu_history)[-n:]
            return round(sum(window) / len(window), 1) if window else None

        load1  = _rolling(60)
        load5  = _rolling(300)
        load15 = _rolling(900)

        # ── RAM ───────────────────────────────────────────────────────────────
        vm = psutil.virtual_memory()
        ram_percent = vm.percent

        # ── Disk usage + I/O rates ────────────────────────────────────────────
        now = time.time()
        disk_usage   = psutil.disk_usage('/')
        disk_read_mbps  = 0.0
        disk_write_mbps = 0.0
        try:
            disk_io = psutil.disk_io_counters()
            if self._last_disk_io is not None and self._last_io_time is not None:
                dt = now - self._last_io_time
                if dt > 0:
                    disk_read_mbps  = round(max(0.0, (disk_io.read_bytes  - self._last_disk_io.read_bytes)  / dt / (1024 * 1024)), 2)
                    disk_write_mbps = round(max(0.0, (disk_io.write_bytes - self._last_disk_io.write_bytes) / dt / (1024 * 1024)), 2)
            self._last_disk_io = disk_io
        except Exception:
            pass

        # ── Network I/O rates ─────────────────────────────────────────────────
        net_tx_mbps = 0.0
        net_rx_mbps = 0.0
        try:
            net_io = psutil.net_io_counters()
            if self._last_net_io is not None and self._last_io_time is not None:
                dt = now - self._last_io_time
                if dt > 0:
                    net_tx_mbps = round(max(0.0, (net_io.bytes_sent - self._last_net_io.bytes_sent) / dt / (1024 * 1024)), 2)
                    net_rx_mbps = round(max(0.0, (net_io.bytes_recv - self._last_net_io.bytes_recv) / dt / (1024 * 1024)), 2)
            self._last_net_io = net_io
        except Exception:
            pass

        self._last_io_time = now

        # ── Temperature ───────────────────────────────────────────────────────
        cpu_c = None
        try:
            temps = psutil.sensors_temperatures()
            for key in ('coretemp', 'cpu-thermal', 'acpitz', 'k10temp', 'cpu_thermal'):
                entries = temps.get(key)
                if entries:
                    cpu_c = round(entries[0].current, 1)
                    break
        except Exception:
            pass

        return {
            'cpu': {
                'now':    cpu_now,
                'load1':  load1,
                'load5':  load5,
                'load15': load15,
            },
            'ram': {
                'percent':  round(ram_percent, 1),
                'used_mb':  round(vm.used  / (1024 * 1024), 1),
                'total_mb': round(vm.total / (1024 * 1024), 1),
            },
            'disk': {
                'percent':    round(disk_usage.percent, 1),
                'used_gb':    round(disk_usage.used  / (1024 ** 3), 2),
                'total_gb':   round(disk_usage.total / (1024 ** 3), 2),
                'read_mbps':  disk_read_mbps,
                'write_mbps': disk_write_mbps,
            },
            'net': {
                'tx_mbps': net_tx_mbps,
                'rx_mbps': net_rx_mbps,
            },
            'temp': {
                'cpu_c': cpu_c,
            },
        }

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _node_full_name(name: str, namespace: str) -> str:
        ns = namespace if namespace.endswith('/') else namespace + '/'
        return ns + name

    @staticmethod
    def _qos_to_dict(qos) -> dict | None:
        if not qos:
            return None
        return {
            'reliability': qos.reliability.name if hasattr(qos.reliability, 'name') else str(qos.reliability),
            'durability':  qos.durability.name  if hasattr(qos.durability,  'name') else str(qos.durability),
            'history':     qos.history.name     if hasattr(qos.history,     'name') else str(qos.history),
            'depth':       qos.depth,
            'liveliness':  qos.liveliness.name  if hasattr(qos.liveliness,  'name') else str(qos.liveliness),
        }

    def _send_event_and_update(self, event: dict, log: str = ''):
        """Queue an event to the WS send loop and mark the graph dirty."""
        if log:
            self.get_logger().debug(log)
        if event:
            self._enqueue(event)
        self._graph_dirty = True

    def _enqueue(self, payload: dict):
        """Thread-safe enqueue to the asyncio send queue."""
        if self.ws and self.loop:
            asyncio.run_coroutine_threadsafe(
                self._send_queue.put(json.dumps(payload)),
                self.loop,
            )

    # ──────────────────────────────────────────────
    # Collector event handlers
    # ──────────────────────────────────────────────

    def _on_ros2_control_event(self, event: dict):
        self._enqueue(event)

    def _on_tf_tree_event(self, event: dict):
        self._enqueue(event)

    def _on_bt_event(self, event: dict):
        if event.get('type') == 'bt_tree':
            self._cached_bt_tree_event = event if event.get('tree_id') else None
        self._enqueue(event)

    # ──────────────────────────────────────────────
    # Nav2 BT monitoring
    # ──────────────────────────────────────────────

    def _init_nav2_bt_monitor(self):
        return  # BT monitoring completely disabled — no subscriptions created
        try:
            from nav2_msgs.msg import BehaviorTreeLog
            from action_msgs.msg import GoalStatusArray
            self._nav2_bt_statuses:        dict[str, str] = {}
            self._nav2_bt_session_active   = False
            self._nav2_bt_publisher_active = False
            self._nav2_bt_tree_id          = None
            self._nav2_bt_tree_structure   = None
            self._nav2_bt_nodes_list:      list = []
            self._nav2_bt_name_to_uid:     dict = {}
            self.create_subscription(
                BehaviorTreeLog, '/behavior_tree_log', self._on_nav2_bt_log, 10
            )
            self.create_subscription(
                GoalStatusArray,
                '/navigate_to_pose/_action/status',
                self._on_nav2_goal_status,
                10,
            )
            # If bt_navigator is already publishing, pre-parse the XML so
            # the startup bt_state event includes the tree structure.
            if self.count_publishers('/behavior_tree_log') > 0:
                self._nav2_bt_publisher_active = True
                self._load_and_parse_bt_xml()
        except Exception as e:
            self.get_logger().debug(f"Nav2 BT monitoring unavailable: {e}")

    def _load_and_parse_bt_xml(self) -> bool:
        if self._nav2_bt_tree_id is not None:
            return True
        import hashlib
        import xml.etree.ElementTree as ET

        xml_path = self._node_parameter_cache.get('/bt_navigator', {}).get(
            'default_nav_to_pose_bt_xml', ''
        )
        if not xml_path:
            try:
                from ament_index_python.packages import get_package_share_directory
                nav2_share = get_package_share_directory('nav2_bt_navigator')
                xml_path = os.path.join(
                    nav2_share, 'behavior_trees',
                    'navigate_to_pose_w_replanning_and_recovery.xml',
                )
            except Exception:
                return False

        try:
            with open(xml_path) as f:
                xml_content = f.read()
        except Exception as e:
            self.get_logger().error(f"Cannot read BT XML '{xml_path}': {e}")
            return False

        try:
            root_elem = ET.fromstring(xml_content)
            bt_elem = root_elem.find('.//BehaviorTree')
            if bt_elem is None:
                return False

            nodes_list: list = []
            name_to_uid: dict = {}
            uid_counter = [1]

            def elem_to_node(elem):
                name = elem.attrib.get('name', elem.attrib.get('ID', elem.tag))
                uid = uid_counter[0]; uid_counter[0] += 1
                name_to_uid[name] = uid
                nodes_list.append({'uid': uid, 'name': name, 'tag': elem.tag})
                node = {
                    'tag': elem.tag, 'name': name, 'uid': uid,
                    'attributes': dict(elem.attrib),
                }
                kids = [elem_to_node(c) for c in elem]
                if kids:
                    node['children'] = kids
                return node

            bt_children = list(bt_elem)
            tree_structure = elem_to_node(bt_children[0]) if bt_children else {}
            self._nav2_bt_tree_structure = tree_structure
            self._nav2_bt_nodes_list     = nodes_list
            self._nav2_bt_name_to_uid    = name_to_uid
            self._nav2_bt_tree_id = hashlib.sha1(xml_content.encode()).hexdigest()[:16]
            return True
        except Exception as e:
            self.get_logger().error(f"Failed to parse BT XML: {e}")
            return False

    def _on_nav2_bt_log(self, msg):
        return  # BISECT R4b: disable BT log forwarding
        if not self._nav2_bt_publisher_active:
            return
        if not self._load_and_parse_bt_xml():
            return

        changes = []
        has_running = False
        for change in msg.event_log:
            self._nav2_bt_statuses[change.node_name] = change.current_status
            uid = self._nav2_bt_name_to_uid.get(change.node_name)
            if uid is not None:
                changes.append({
                    'uid': uid, 'name': change.node_name,
                    'tag': '', 'status': change.current_status,
                    'previous_status': change.previous_status,
                })
            if change.current_status == 'RUNNING':
                has_running = True

        if has_running and not self._nav2_bt_session_active:
            self.get_logger().info("[bt] navigation session started")
            self._nav2_bt_session_active = True
            self._on_bt_event({
                'type': 'bt_tree', 'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'tree': self._nav2_bt_tree_structure,
                'nodes': [
                    {**nd, 'status': self._nav2_bt_statuses.get(nd['name'], 'IDLE')}
                    for nd in self._nav2_bt_nodes_list
                ],
            })

        if changes:
            self._on_bt_event({
                'type': 'bt_status', 'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'changes': changes,
            })

    def _on_nav2_goal_status(self, msg):
        has_active = any(s.status in (1, 2, 3) for s in msg.status_list)
        if self._nav2_bt_session_active and not has_active:
            self.get_logger().info("[bt] navigation session ended")
            self._nav2_bt_session_active = False
            self._nav2_bt_statuses.clear()
            if self._nav2_bt_tree_id:
                self._on_bt_event({
                    'type': 'bt_tree', 'timestamp': time.time(),
                    'tree_id': self._nav2_bt_tree_id,
                    'tree': self._nav2_bt_tree_structure,
                    'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                })

    def _on_nav2_bt_gone(self):
        if self._nav2_bt_tree_id is None and not self._nav2_bt_publisher_active:
            return
        self.get_logger().info("Nav2 BT gone — clearing BT state")
        self._nav2_bt_publisher_active = False
        self._nav2_bt_session_active   = False
        self._nav2_bt_statuses.clear()
        self._nav2_bt_tree_id          = None
        self._nav2_bt_tree_structure   = None
        self._nav2_bt_nodes_list       = []
        self._nav2_bt_name_to_uid      = {}
        self._on_bt_event({
            'type': 'bt_tree', 'timestamp': time.time(),
            'tree_id': None, 'tree': None, 'nodes': [],
        })

    def _build_startup_bt_state_event(self) -> dict:
        src = self._cached_bt_tree_event
        if src:
            return {
                'type': 'bt_state', 'timestamp': src.get('timestamp', time.time()),
                'tree_id': src.get('tree_id'), 'tree': src.get('tree'),
                'nodes': src.get('nodes', []),
            }
        if (
            hasattr(self, '_nav2_bt_tree_id')
            and self._nav2_bt_tree_id
            and self._nav2_bt_tree_structure
        ):
            return {
                'type': 'bt_state', 'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'tree': self._nav2_bt_tree_structure,
                'nodes': [
                    {**nd, 'status': self._nav2_bt_statuses.get(nd['name'], 'IDLE')}
                    for nd in self._nav2_bt_nodes_list
                ],
            }
        return {
            'type': 'bt_state', 'timestamp': time.time(),
            'tree_id': None, 'tree': None, 'nodes': [],
        }

    # ──────────────────────────────────────────────
    # Cleanup
    # ──────────────────────────────────────────────

    def destroy_node(self):
        self._ros2_control.destroy()
        if self._tf_tree is not None:
            self._tf_tree.destroy()
        if self._bt_collector:
            self._bt_collector.stop()
        super().destroy_node()


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

def main(args=None):
    rclpy.init(args=args)
    node = WebBridge()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, rclpy.executors.ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
