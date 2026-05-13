import asyncio
import os
import platform
import random
import sys
import threading
import time
from collections import deque

import psutil
import rclpy
import websockets
import json

from rcl_interfaces.msg import ParameterEvent
from rcl_interfaces.srv import GetParameters, ListParameters
from rclpy.node import Node
from std_msgs.msg import Empty as EmptyMsg
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
TELEMETRY_INTERVAL         = 1.0   # seconds between telemetry samples
TF_TREE_INTERVAL           = 0.2   # seconds between tf_tree polls (5 Hz)
SERVICE_SCAN_INTERVAL      = 30.0  # seconds between service graph scans
MAX_SUBSCRIPTIONS          = 100   # hard cap on gateway-requested topic subs
RECONNECT_INITIAL_DELAY    = 1     # seconds
RECONNECT_MAX_DELAY        = 30    # seconds

# Services to suppress from graph output (internal ROS2 plumbing)
_SUPPRESSED_SERVICE_PREFIXES = ('/ros2cli_daemon',)

ACTION_FEEDBACK_MIN_INTERVAL = 0.2  # seconds between forwarded feedback messages per action (5 Hz cap)

class WebBridge(Node):

    def __init__(self, watcher_proc=None):
        super().__init__('osiris_node')
        self._watcher_proc = watcher_proc

        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")

        # Declare tunable parameters
        self.declare_parameter('graph_check_interval', GRAPH_CHECK_INTERVAL)
        self.declare_parameter('telemetry_interval',   TELEMETRY_INTERVAL)
        self.declare_parameter('tf_tree_enabled',      False)
        self.declare_parameter('battery_topic',        '/battery_state')

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

        # ── Existence caches (set of fully-qualified names) ───────────────────
        self._active_nodes:    set[str] = set()
        self._active_topics:   set[str] = set()
        self._active_services: dict[str, str] = {}
        self._active_actions:  set[str] = set()

        # ── Action type cache ─────────────────────────────────────────────────
        # None  = fetch attempted but failed (package not installed, etc.)
        # dict  = { goal_type, result_type, feedback_type,
        #           goal_fields, result_fields, feedback_fields }
        self._action_type_cache: dict[str, dict | None] = {}

        # ── Action monitoring (status + feedback) ─────────────────────────────
        self._action_status_subs:       dict[str, rclpy.subscription.Subscription] = {}
        self._action_feedback_subs:     dict[str, rclpy.subscription.Subscription] = {}
        self._action_goal_states:       dict[str, dict[str, int]] = {}  # action → {uuid_hex → status_int}
        self._action_feedback_throttle: dict[str, float] = {}           # action → last_sent_time

        # ── Count sentinels (cheap change detection) ─────────────────────────
        self._topic_counts: dict[str, tuple[int, int]] = {}  # topic → (pub_n, sub_n)

        # ── Relation caches (populated by Tier-2 enrichment) ─────────────────
        self._topic_relations: dict[str, dict] = {}

        # ── Enrichment pending queues ─────────────────────────────────────────
        self._pending_topic_enrichment: set[str] = set()

        # ── Parameters (lazy-loaded, async) ──────────────────────────────────
        self._node_parameter_cache: dict[str, dict | None] = {}  # None = not yet fetched, {} = fetched but empty
        self._pending_param_fetches: set[str] = set()
        self._nodes_no_param_service: set[str] = set()  # nodes whose list_parameters was never ready

        # ── Lifecycle subscriptions (auto-detected managed nodes) ─────────────
        self._lifecycle_subs: dict[str, rclpy.subscription.Subscription] = {}  # topic → sub
        self._lifecycle_state_cache: dict[str, str] = {}   # node_fqn → state label
        self._pending_lifecycle_fetches: set[str] = set()  # node_fqns with in-flight get_state calls

        # ── Snapshot & dirty-flag ─────────────────────────────────────────────
        self._last_sent_nodes:    dict | None = None
        self._last_sent_topics:   dict | None = None
        self._last_sent_actions:  dict | None = None
        self._last_sent_services: dict | None = None
        self._graph_dirty = False
        self._graph_debounce_timer: threading.Timer | None = None

        # ── Service scan throttle ─────────────────────────────────────────────
        self._last_service_scan: float = 0.0
        self._service_rescan_ticks: int = 0

        # ── Initial scan synchronization ──────────────────────────────────────
        self._initial_scan_complete = threading.Event()
        self._first_graph_check_done = False
        self._graph_check_lock = threading.Lock()  # serializes concurrent _check_graph_changes calls
        self._param_fetch_timer = None  # one-shot timer for delayed initial param fetch
        # ── BT state ─────────────────────────────────────────────────────────
        self._cached_bt_tree_event: dict | None = None

        # ── Telemetry ─────────────────────────────────────────────────────────
        self._last_disk_io      = None
        self._last_net_io       = None
        self._last_io_time:     float | None = None
        self._last_battery_state: dict | None = None
        psutil.cpu_percent(interval=None)  # prime — first call always returns 0.0

        # ── Collectors ────────────────────────────────────────────────────────
        ros2_control_enabled = os.environ.get(
            'OSIRIS_ROS2_CONTROL_ENABLED', ''
        ).lower() in ('true', '1', 'yes')
        if ros2_control_enabled:
            self._ros2_control = Ros2ControlCollector(
                node=self,
                event_callback=self._on_ros2_control_event,
                logger=self.get_logger(),
            )
        else:
            self._ros2_control = None
        _tf_tree_enabled = self.get_parameter('tf_tree_enabled').get_parameter_value().bool_value
        self._tf_tree = TfTreeCollector(
            node=self,
            event_callback=self._on_tf_tree_event,
            logger=self.get_logger(),
        ) if _tf_tree_enabled else None

        # ── Timers ────────────────────────────────────────────────────────────
        _graph_interval     = self.get_parameter('graph_check_interval').get_parameter_value().double_value
        _telemetry_interval = self.get_parameter('telemetry_interval').get_parameter_value().double_value

        # Subscribe to C++ graph watcher events — event-driven polls.
        # The one-shot startup timer guarantees an initial scan even when the
        # C++ watcher binary is unavailable (e.g. pip install without binary).
        # Use VOLATILE (default) depth=10: the startup timer handles initial state,
        # and live events are reliably received without TRANSIENT_LOCAL replay
        # which would race with the 1s startup timer on a background thread.
        self.create_subscription(
            EmptyMsg, '/osiris/graph_changed',
            self._on_graph_changed, 10,
        )
        self.create_subscription(
            ParameterEvent, '/parameter_events',
            self._on_parameter_event, 100,
        )
        self._startup_check_timer = self.create_timer(1.0, self._do_startup_check)
        self.create_timer(_telemetry_interval,         self._collect_telemetry)
        self.create_timer(TF_TREE_INTERVAL,             self._poll_tf_tree)

        # ── Battery state subscription ────────────────────────────────────────
        _battery_topic = self.get_parameter('battery_topic').get_parameter_value().string_value
        try:
            from sensor_msgs.msg import BatteryState as BatteryStateMsg
            self.create_subscription(
                BatteryStateMsg, _battery_topic,
                self._on_battery_state, 10,
            )
            self.get_logger().info(f'Battery state subscription active on {_battery_topic}')
        except Exception as e:
            self.get_logger().warning(f'Battery state monitoring unavailable: {e}')

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

        _watcher_status = (
            f'pid={watcher_proc.pid}' if watcher_proc is not None else 'not started'
        )
        self.get_logger().info(
            f"🚀 Osiris agent v{AGENT_VERSION} — event based graph monitoring "
            f"(graph_watcher {_watcher_status})"
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
            elif msg_type == 'error':
                self.get_logger().warning(f"Gateway error: {data.get('message', '')}")

    async def _send_initial_state(self):
        # Wait for the first _check_graph_changes tick to populate all caches.
        await asyncio.to_thread(self._initial_scan_complete.wait, 15.0)

        # Reset delta caches so _flush_graph_snapshots treats everything as
        # "unsent" after this reconnect.
        self._last_sent_nodes    = None
        self._last_sent_topics   = None
        self._last_sent_actions  = None
        self._last_sent_services = None
        self._graph_dirty        = True

        nodes       = self._get_nodes_with_relations()
        topics      = self._get_topics_with_relations()
        actions     = self._get_actions_with_relations()
        services    = self._get_services_with_relations()
        controllers = self._ros2_control.get_controllers_snapshot() if self._ros2_control is not None else []
        hardware    = self._ros2_control.get_hardware_snapshot() if self._ros2_control is not None else []
        telemetry   = self._get_telemetry_snapshot()
        tf_tree     = self._tf_tree.get_snapshot() if self._tf_tree is not None else None
        bt_state    = self._build_startup_bt_state_event()
        bt          = self._bt_snapshot_from_state_event(bt_state)
        initial_timestamp = time.time()

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
            'timestamp': initial_timestamp,
            'data': {
                'timestamp': initial_timestamp,
                'graph': {
                    'nodes':       nodes,
                    'topics':      topics,
                    'actions':     actions,
                    'services':    services,
                    'controllers': controllers,
                    'hardware':    hardware,
                },
                'meta':      self._get_initial_state_meta(telemetry),
                'telemetry': telemetry,
                'tf_tree':   tf_tree,
                'bt':        bt,
            },
        }))

        await self._send_queue.put(json.dumps(bt_state))

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
        if not self._graph_check_lock.acquire(blocking=False):
            return  # another poll is already in progress — skip this one
        try:
            self._check_graph_changes_locked()
        finally:
            self._graph_check_lock.release()

    def _check_graph_changes_locked(self):
        # ── 1. Node + topic queries (always, both cheap) ──────────────────────
        _t0 = time.time()
        node_pairs      = list(self.get_node_names_and_namespaces())
        topic_type_list = self.get_topic_names_and_types()
        _t1 = time.time()

        # Build a flat map for O(1) type lookup throughout this method
        topic_type_map = {t: types for t, types in topic_type_list}

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
        _now = time.time()
        _nodes_stopped  = self._first_graph_check_done and bool(self._active_nodes - current_nodes)
        _nodes_started  = self._first_graph_check_done and bool(current_nodes - self._active_nodes)
        _do_service_scan = (
            not self._first_graph_check_done
            or _nodes_stopped
            or _nodes_started
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
        if not self._first_graph_check_done:
            self._first_graph_check_done = True
            self._active_nodes    = current_nodes
            self._active_topics   = current_topics
            self._active_services = current_services
            self._active_actions  = current_actions
            _te0 = time.time()
            self._do_full_initial_enrichment(topic_type_list, node_pairs)
            _te1 = time.time()
            # Stagger parameter fetches: fire them on the ROS executor thread
            # 5 s after the first tick so we don't hammer lifecycle nodes that
            # are still in the middle of configuring/activating.
            def _fetch_all_params_delayed():
                for fqn in list(current_nodes):
                    self._fetch_node_parameters_async(fqn)
            self._param_fetch_timer = self.create_timer(5.0, lambda: (self._cancel_param_fetch_timer(), _fetch_all_params_delayed()))
            for _t in current_topics:
                if _t.endswith('/transition_event'):
                    self._subscribe_lifecycle_topic(_t)
                    self._fetch_lifecycle_state_async(_t[:-len('/transition_event')])
            # Resolve action types and subscribe monitoring for all actions at startup
            for a in current_actions:
                self._fetch_action_types(a, topic_type_map)
                self._subscribe_action_status(a)
            if self._ros2_control is not None:
                self._ros2_control.poll(force=True)
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
            self._pending_topic_enrichment.update(self._active_topics)
            self._graph_dirty = True
        for fqn in started_nodes:
            self._nodes_no_param_service.discard(fqn)  # allow retry after restart
            self._fetch_node_parameters_async(fqn)
            self._fetch_lifecycle_state_async(fqn)

        stopped_nodes = self._active_nodes - current_nodes
        if stopped_nodes:
            self.get_logger().info(f"[poll] {len(stopped_nodes)} node(s) stopped: {sorted(stopped_nodes)}")
            self._graph_dirty = True
        for fqn in stopped_nodes:
            for topic, rel in self._topic_relations.items():
                if fqn in rel.get('publishers', set()) or fqn in rel.get('subscribers', set()):
                    self._pending_topic_enrichment.add(topic)
            self._node_parameter_cache.pop(fqn, None)
            self._pending_param_fetches.discard(fqn)
            self._lifecycle_state_cache.pop(fqn, None)
            self._pending_lifecycle_fetches.discard(fqn)

        # ── 3. Topic events ───────────────────────────────────────────────────
        for t in current_topics - self._active_topics:
            self._pending_topic_enrichment.add(t)
            self._graph_dirty = True
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
            self._graph_dirty = True
            if t.endswith('/transition_event'):
                lc_sub = self._lifecycle_subs.pop(t, None)
                if lc_sub:
                    self.destroy_subscription(lc_sub)
            if t == '/behavior_tree_log' and hasattr(self, '_nav2_bt_tree_id'):
                self._on_nav2_bt_gone()

        # ── 4. Service changes (only every SERVICE_SCAN_INTERVAL) ─────────────
        if _do_service_scan:
            if set(current_services) != set(self._active_services):
                self._graph_dirty = True

        # ── 5. Action events ──────────────────────────────────────────────────
        for a in current_actions - self._active_actions:
            self._fetch_action_types(a, topic_type_map)
            self._subscribe_action_status(a)
            self._graph_dirty = True

        for a in self._active_actions - current_actions:
            self._action_type_cache.pop(a, None)
            self._unsubscribe_action_monitoring(a)
            self._graph_dirty = True

        # ── 6. Update existence caches ────────────────────────────────────────
        self._active_nodes    = current_nodes
        self._active_topics   = current_topics
        if _do_service_scan:
            self._active_services = current_services
        self._active_actions  = current_actions

        # ── 7. Re-enrich only topics whose pub/sub count changed ─────────────
        if self._pending_topic_enrichment:
            self._enrich_pending_relations(topic_type_list)

        # ── 8. Retry action type resolution for any unresolved actions ────────
        for a in current_actions:
            if a not in self._action_type_cache:
                self._fetch_action_types(a, topic_type_map)

        # ── 9. Nav2 BT publisher liveness check ──────────────────────────────
        if hasattr(self, '_nav2_bt_publisher_active'):
            bt_rel = self._topic_relations.get('/behavior_tree_log', {})
            bt_pubs = bt_rel.get('publishers', set()) & current_nodes
            if self._nav2_bt_publisher_active and not bt_pubs:
                self._on_nav2_bt_gone()
            elif self._nav2_bt_publisher_active and bt_pubs and self._nav2_bt_tree_id is None:
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

        # ── 11. Poll collectors ───────────────────────────────────────────────
        if self._ros2_control is not None:
            self._ros2_control.poll()
        if self._tf_tree is not None:
            self._tf_tree.poll()

    # ──────────────────────────────────────────────
    # Initial full enrichment (called once on first tick)
    # ──────────────────────────────────────────────

    def _do_full_initial_enrichment(self, topic_type_list, node_pairs):
        topic_type_map = dict(topic_type_list)
        self._pending_topic_enrichment.clear()
        for topic in self._active_topics:
            try:
                pub_infos = self.get_publishers_info_by_topic(topic)
                sub_infos = self.get_subscriptions_info_by_topic(topic)
            except Exception:
                continue
            publishers  = {self._node_full_name(p.node_name, p.node_namespace) for p in pub_infos}
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
    # Tier-2: batched relation enrichment (inert with R1 gate)
    # ──────────────────────────────────────────────

    def _enrich_pending_relations(self, topic_type_list=None):
        if not self._pending_topic_enrichment:
            return

        batch = set(self._pending_topic_enrichment)
        self._pending_topic_enrichment.clear()
        _t0 = time.time()
        self.get_logger().info(f"[enrich] {len(batch)} topics")

        if topic_type_list is not None:
            topic_type_map = dict(topic_type_list)
        else:
            topic_type_map = dict(self.get_topic_names_and_types())

        for topic in batch:
            if topic not in self._active_topics:
                continue
            try:
                pub_infos = self.get_publishers_info_by_topic(topic)
                sub_infos = self.get_subscriptions_info_by_topic(topic)
            except Exception as e:
                self.get_logger().debug(f"Enrichment failed for {topic}: {e}")
                continue

            publishers  = {self._node_full_name(p.node_name, p.node_namespace) for p in pub_infos}
            subscribers = {self._node_full_name(s.node_name, s.node_namespace) for s in sub_infos}
            old = self._topic_relations.get(topic)
            new_rel = {
                'publishers':       publishers,
                'subscribers':      subscribers,
                'publisher_infos':  pub_infos,
                'subscriber_infos': sub_infos,
                'type': topic_type_map.get(topic, ['unknown'])[0],
            }
            self._topic_relations[topic] = new_rel
            self._topic_counts[topic] = (len(pub_infos), len(sub_infos))

            if old is not None:
                if subscribers != old['subscribers']:
                    self._graph_dirty = True

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
        for topic in self._active_topics:
            rel = self._topic_relations.get(topic, {})
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
        for action in self._active_actions:
            rel = self._topic_relations.get(f'{action}/_action/status', {})
            providers = [
                self._node_full_name(p.node_name, p.node_namespace)
                for p in rel.get('publisher_infos', [])
            ]
            result[action] = {
                'providers': providers,
                **(self._action_type_cache.get(action) or {}),
            }
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

        nodes = self._get_nodes_with_relations()
        if nodes != self._last_sent_nodes:
            self.get_logger().info(f"[flush] nodes ({len(nodes)} nodes)")
            self._last_sent_nodes = nodes.copy()
            self._enqueue({'type': 'nodes', 'data': nodes, 'timestamp': time.time()})

        topics = self._get_topics_with_relations()
        if topics != self._last_sent_topics:
            self.get_logger().info(f"[flush] topics ({len(topics)} topics)")
            self._last_sent_topics = topics.copy()
            self._enqueue({'type': 'topics', 'data': topics, 'timestamp': time.time()})

        actions = self._get_actions_with_relations()
        if actions != self._last_sent_actions:
            self.get_logger().info(f"[flush] actions ({len(actions)} actions)")
            self._last_sent_actions = actions.copy()
            self._enqueue({'type': 'actions', 'data': actions, 'timestamp': time.time()})

        services = self._get_services_with_relations()
        if services != self._last_sent_services:
            self.get_logger().info(f"[flush] services ({len(services)} services)")
            self._last_sent_services = services.copy()
            self._enqueue({'type': 'services', 'data': services, 'timestamp': time.time()})

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
    # Lifecycle (managed nodes)
    # ──────────────────────────────────────────────

    def _subscribe_lifecycle_topic(self, topic: str):
        """Subscribe to a /<node>/transition_event topic."""
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

    def _fetch_lifecycle_state_async(self, node_fqn: str):
        """Query /<node>/get_state to populate _lifecycle_state_cache."""
        if node_fqn in self._lifecycle_state_cache:
            return
        if node_fqn in self._pending_lifecycle_fetches:
            return
        try:
            from lifecycle_msgs.srv import GetState
        except ImportError:
            return
        client = self.create_client(GetState, f'{node_fqn}/get_state')
        if not client.service_is_ready():
            self.destroy_client(client)
            return
        self._pending_lifecycle_fetches.add(node_fqn)
        future = client.call_async(GetState.Request())

        def _on_get_state(fut):
            self._pending_lifecycle_fetches.discard(node_fqn)
            self.destroy_client(client)
            try:
                resp = fut.result()
                if resp is not None:
                    self._lifecycle_state_cache[node_fqn] = resp.current_state.label
                    self._graph_dirty = True
                    self.get_logger().debug(
                        f'[lifecycle] initial state for {node_fqn}: {resp.current_state.label}'
                    )
            except Exception as e:
                self.get_logger().debug(f'[lifecycle] get_state failed for {node_fqn}: {e}')

        future.add_done_callback(_on_get_state)

    def _on_lifecycle_transition(self, msg, node_fqn: str):
        self._lifecycle_state_cache[node_fqn] = msg.goal_state.label
        self.get_logger().info(
            f'[lifecycle] {node_fqn}: {msg.start_state.label} → {msg.goal_state.label} '
            f'(transition: {msg.transition.label})'
        )
        self._enqueue({
            'type': 'lifecycle_event',
            'node': node_fqn,
            'transition': msg.transition.label,
            'from_state': msg.start_state.label,
            'to_state': msg.goal_state.label,
            'timestamp': time.time(),
        })
        self._graph_dirty = True
        self._trigger_graph_poll()

    # ──────────────────────────────────────────────
    # Action monitoring (status + feedback)
    # ──────────────────────────────────────────────

    def _subscribe_action_status(self, action_name: str):
        if action_name in self._action_status_subs:
            return
        try:
            from action_msgs.msg import GoalStatusArray
            sub = self.create_subscription(
                GoalStatusArray,
                f'{action_name}/_action/status',
                lambda msg, a=action_name: self._on_action_status(msg, a),
                QoSProfile(depth=10),
            )
            self._action_status_subs[action_name] = sub
            self.get_logger().info(f'[actions] subscribed to status for {action_name}')
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to subscribe to status for {action_name}: {e}')

    def _subscribe_action_feedback(self, action_name: str, feedback_msg_cls):
        if action_name in self._action_feedback_subs:
            return
        try:
            sub = self.create_subscription(
                feedback_msg_cls,
                f'{action_name}/_action/feedback',
                lambda msg, a=action_name: self._on_action_feedback(msg, a),
                QoSProfile(depth=10),
            )
            self._action_feedback_subs[action_name] = sub
            self.get_logger().info(f'[actions] subscribed to feedback for {action_name}')
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to subscribe to feedback for {action_name}: {e}')

    def _unsubscribe_action_monitoring(self, action_name: str):
        sub = self._action_status_subs.pop(action_name, None)
        if sub:
            self.destroy_subscription(sub)
        sub = self._action_feedback_subs.pop(action_name, None)
        if sub:
            self.destroy_subscription(sub)
        self._action_goal_states.pop(action_name, None)
        self._action_feedback_throttle.pop(action_name, None)

    def _on_action_status(self, msg, action_name: str):
        prev = self._action_goal_states.get(action_name, {})
        current = {bytes(s.goal_info.goal_id.uuid).hex(): s.status for s in msg.status_list}
        for uuid_hex, status in current.items():
            if uuid_hex not in prev or prev[uuid_hex] != status:
                self._enqueue({
                    'type': 'goal_event',
                    'action': action_name,
                    'goal_id': uuid_hex,
                    'status': status,
                    'timestamp': time.time(),
                })
        self._action_goal_states[action_name] = current

    def _on_action_feedback(self, msg, action_name: str):
        now = time.time()
        if now - self._action_feedback_throttle.get(action_name, 0.0) < ACTION_FEEDBACK_MIN_INTERVAL:
            return
        self._action_feedback_throttle[action_name] = now
        try:
            feedback_data = message_to_ordereddict(msg.feedback)
        except Exception as e:
            self.get_logger().warning(f'[actions] feedback serialization failed for {action_name}: {e}')
            return
        self._enqueue({
            'type': 'action_feedback',
            'action': action_name,
            'goal_id': bytes(msg.goal_id.uuid).hex(),
            'feedback': feedback_data,
            'timestamp': now,
        })

    # ──────────────────────────────────────────────
    # C++ graph watcher integration
    # ──────────────────────────────────────────────

    def _on_graph_changed(self, _msg: EmptyMsg):
        """Debounced callback fired by the C++ osiris_graph_watcher node."""
        self.get_logger().info("[graph] event received")
        self._trigger_graph_poll()

    def _trigger_graph_poll(self):
        """Single debounced entry point for all graph poll triggers.

        Resets a 1s one-shot timer on every call so rapid bursts coalesce
        into a single poll.
        """
        if self._graph_debounce_timer is not None:
            self._graph_debounce_timer.cancel()
        self._graph_debounce_timer = threading.Timer(1.0, self._debounce_fire)
        self._graph_debounce_timer.daemon = True
        self._graph_debounce_timer.start()

    def _debounce_fire(self):
        """Called from threading.Timer — run the graph poll directly."""
        self.get_logger().info("[graph] watcher triggered poll")
        self._check_graph_changes()

    # ──────────────────────────────────────────────
    # Parameters (async, lazy-loaded)
    # ──────────────────────────────────────────────

    def _do_startup_check(self):
        """One-shot timer: run the initial graph scan then cancel itself."""
        self._startup_check_timer.cancel()
        if self._watcher_proc is not None:
            rc = self._watcher_proc.poll()
            if rc is not None:
                self.get_logger().error(
                    f"[graph] graph_watcher exited unexpectedly (rc={rc}) — "
                    "no graph events will be received"
                )
            else:
                self.get_logger().info(
                    f"[graph] graph_watcher healthy (pid={self._watcher_proc.pid})"
                )
        if not self._first_graph_check_done:
            self._check_graph_changes()

    def _cancel_param_fetch_timer(self):
        """Cancel the one-shot delayed param-fetch timer after it fires."""
        t = self._param_fetch_timer
        if t is not None:
            t.cancel()
            self._param_fetch_timer = None

    def _on_parameter_event(self, msg: ParameterEvent):
        """React to parameter changes published by any node on /parameter_events."""
        fqn = msg.node
        if fqn not in self._active_nodes:
            return
        cache = dict(self._node_parameter_cache.get(fqn) or {})
        for param in list(msg.new_parameters) + list(msg.changed_parameters):
            try:
                cache[param.name] = parameter_value_to_python(param.value)
            except Exception:
                pass
        for param in msg.deleted_parameters:
            cache.pop(param.name, None)
        if cache != self._node_parameter_cache.get(fqn):
            self._node_parameter_cache[fqn] = cache
            self.get_logger().debug(f'[params] updated {len(cache)} params for {fqn} via /parameter_events')
            self._trigger_graph_poll()

    def _fetch_action_types(self, action_name: str, topic_type_map: dict) -> bool:
        """Resolve and cache goal/result/feedback types for an action server.

        Looks up the [action]/_action/feedback topic type, strips the
        _FeedbackMessage suffix to derive the base action type, then imports
        the _Goal / _Result / _Feedback message classes to introspect fields.

        Returns True if types were resolved and enqueued, False if not yet
        available (e.g. the feedback topic hasn't appeared in DDS yet).
        """
        if action_name in self._action_type_cache:
            return self._action_type_cache[action_name] is not None

        feedback_topic = f'{action_name}/_action/feedback'
        types_list = topic_type_map.get(feedback_topic)
        if not types_list:
            return False

        feedback_msg_type = types_list[0]
        if not feedback_msg_type.endswith('_FeedbackMessage'):
            return False

        base_type = feedback_msg_type[:-len('_FeedbackMessage')]

        try:
            # base_type is e.g. 'nav2_msgs/action/NavigateToPose'
            # Action sub-types (Goal/Result/Feedback) are nested on the action
            # class itself — get_message() only handles message types, not actions.
            import importlib
            pkg, _, action_name_part = base_type.split('/', 2)
            # action_name_part may be 'action/NavigateToPose' — take just the class name
            class_name = action_name_part.split('/')[-1]
            action_mod = importlib.import_module(f'{pkg}.action')
            action_cls = getattr(action_mod, class_name)
            # Use get_message() for the _FeedbackMessage type — it IS a standalone message
            # type (unlike _Goal/_Result/_Feedback which are nested). The type string
            # comes directly from the topic registry so it's always correct.
            feedback_msg_cls = get_message(feedback_msg_type)

            goal_cls     = action_cls.Goal
            result_cls   = action_cls.Result
            feedback_cls = action_cls.Feedback

            def _fields(cls):
                try:
                    return dict(cls.get_fields_and_field_types())
                except Exception:
                    return {}

            type_info = {
                'goal_type':       f'{base_type}_Goal',
                'result_type':     f'{base_type}_Result',
                'feedback_type':   f'{base_type}_Feedback',
                'goal_fields':     _fields(goal_cls),
                'result_fields':   _fields(result_cls),
                'feedback_fields': _fields(feedback_cls),
            }
            self._action_type_cache[action_name] = type_info
            self.get_logger().info(f'[actions] resolved types for {action_name}: {base_type}')
            self._graph_dirty = True
            self._subscribe_action_feedback(action_name, feedback_msg_cls)
            return True
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to resolve types for {action_name}: {e}')
            self._action_type_cache[action_name] = None  # mark failed — avoid retry spam
            return False

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
            self._nodes_no_param_service.add(fqn)
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
                self._node_parameter_cache[fqn] = {}  # fetched but empty — stop retrying
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

    # ──────────────────────────────────────────────
    # Telemetry
    # ──────────────────────────────────────────────

    def _collect_telemetry(self):
        if not self.ws or not self.loop:
            return
        self._enqueue({
            'type': 'telemetry',
            'data': self._get_telemetry_snapshot(),
            'timestamp': time.time(),
        })

    def _on_battery_state(self, msg) -> None:
        """Cache the latest BatteryState message for inclusion in telemetry snapshots."""
        try:
            self._last_battery_state = {
                'percent':  round(float(msg.percentage) * 100.0, 1) if msg.percentage == msg.percentage else None,  # NaN guard
                'voltage':  round(float(msg.voltage), 3)  if msg.voltage  == msg.voltage  else None,
                'current':  round(float(msg.current), 3)  if msg.current  == msg.current  else None,
                'status':   int(msg.power_supply_status),
                'present':  bool(msg.present),
            }
        except Exception:
            pass

    def _get_telemetry_snapshot(self) -> dict:
        cpu_now = round(psutil.cpu_percent(interval=None), 1)

        vm = psutil.virtual_memory()
        ram_percent = vm.percent

        now = time.time()
        disk_usage      = psutil.disk_usage('/')
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
                'now':       cpu_now,
                'throttling': None,
                'temp':      cpu_c,
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
            'battery': self._last_battery_state,
        }

    def _get_cpu_model(self) -> str | None:
        try:
            if os.path.exists('/proc/cpuinfo'):
                with open('/proc/cpuinfo') as f:
                    for line in f:
                        if line.lower().startswith(('model name', 'hardware', 'processor')):
                            _, value = line.split(':', 1)
                            value = value.strip()
                            if value:
                                return value
        except Exception:
            pass

        cpu_model = platform.processor() or platform.machine()
        return cpu_model or None

    def _get_robot_model(self) -> str | None:
        for env_name in ('OSIRIS_ROBOT_MODEL', 'ROBOT_MODEL'):
            value = os.environ.get(env_name)
            if value:
                return value

        for path in ('/proc/device-tree/model', '/sys/firmware/devicetree/base/model'):
            try:
                if os.path.exists(path):
                    with open(path, 'rb') as f:
                        value = f.read().decode(errors='ignore').strip('\x00\n ')
                        if value:
                            return value
            except Exception:
                pass
        return None

    def _get_initial_state_meta(self, telemetry: dict | None = None) -> dict:
        ram_total_mb = None
        try:
            ram_total_mb = telemetry.get('ram', {}).get('total_mb') if telemetry else None
            if ram_total_mb is None:
                ram_total_mb = round(psutil.virtual_memory().total / (1024 * 1024), 1)
        except Exception:
            pass

        return {
            'agentVersion': AGENT_VERSION,
            'ros_distro': os.environ.get('ROS_DISTRO'),
            'cpu_model': self._get_cpu_model(),
            'cpu_cores': psutil.cpu_count(logical=False),
            'cpu_threads': psutil.cpu_count(logical=True),
            'ram_total_mb': ram_total_mb,
            'arch': platform.machine() or None,
            'robot_model': self._get_robot_model(),
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

    def _poll_tf_tree(self):
        """Periodic 1 Hz timer callback to keep tf_tree updates flowing."""
        if self._tf_tree is not None:
            self._tf_tree.poll()

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
        try:
            from nav2_msgs.msg import BehaviorTreeLog
            from action_msgs.msg import GoalStatusArray
            self._nav2_bt_statuses:           dict[str, str] = {}
            self._nav2_bt_last_sent_statuses:  dict[str, str] = {}  # what client currently has
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
        if not self._nav2_bt_publisher_active:
            return
        if not self._load_and_parse_bt_xml():
            return

        # Collapse all transitions in this log tick to the final status per node.
        # The event_log can contain multiple entries for the same node (e.g.
        # RUNNING → FAILURE → IDLE) — only the last one matters to the client.
        has_running = False
        final_per_node: dict[str, str] = {}  # node_name → final status this tick
        for change in msg.event_log:
            self._nav2_bt_statuses[change.node_name] = change.current_status
            final_per_node[change.node_name] = change.current_status
            if change.current_status == 'RUNNING':
                has_running = True

        if has_running and not self._nav2_bt_session_active:
            self.get_logger().info("[bt] navigation session started")
            self._nav2_bt_session_active = True
            # Full tree send — sync last-sent cache
            self._nav2_bt_last_sent_statuses = {
                nd['name']: self._nav2_bt_statuses.get(nd['name'], 'IDLE')
                for nd in self._nav2_bt_nodes_list
            }
            self._on_bt_event({
                'type': 'bt_tree', 'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'tree': self._nav2_bt_tree_structure,
                'nodes': [
                    {**nd, 'status': self._nav2_bt_last_sent_statuses.get(nd['name'], 'IDLE')}
                    for nd in self._nav2_bt_nodes_list
                ],
            })
            return  # full tree already sent; skip bt_status this tick

        # Only send nodes whose final status this tick differs from what client has
        changes = []
        for node_name, status in final_per_node.items():
            if self._nav2_bt_last_sent_statuses.get(node_name) == status:
                continue
            uid = self._nav2_bt_name_to_uid.get(node_name)
            if uid is not None:
                changes.append({'uid': uid, 'name': node_name, 'tag': '', 'status': status})
                self._nav2_bt_last_sent_statuses[node_name] = status

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
        self._nav2_bt_last_sent_statuses.clear()
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

    def _bt_snapshot_from_state_event(self, src: dict) -> dict:
        return {
            'timestamp': src.get('timestamp', time.time()),
            'tree_id': src.get('tree_id'),
            'tree': src.get('tree'),
            'nodes': src.get('nodes', []),
        }

    # ──────────────────────────────────────────────
    # Cleanup
    # ──────────────────────────────────────────────

    def destroy_node(self):
        # Cancel pending debounce timer so it doesn't fire after shutdown.
        if self._graph_debounce_timer is not None:
            try:
                self._graph_debounce_timer.cancel()
            except Exception:
                pass
            self._graph_debounce_timer = None
        if self._tf_tree is not None:
            self._tf_tree.destroy()
        if self._bt_collector:
            self._bt_collector.stop()
        super().destroy_node()


def main(args=None):
    import shutil
    import subprocess
    import importlib.resources

    # Locate the graph_watcher binary:
    # 1. Prefer PATH (colcon dev workspace with source install/setup.bash)
    # 2. Fall back to arch-specific binary bundled in the pip package:
    #    bin/graph_watcher_x86_64  or  bin/graph_watcher_aarch64
    # 3. Fall back to bin/graph_watcher (legacy / colcon-installed generic name)
    _watcher_proc = None
    _watcher_bin = shutil.which('graph_watcher')
    if _watcher_bin is None:
        try:
            _arch = platform.machine()  # 'x86_64' or 'aarch64'
            _pkg = importlib.resources.files('osiris_agent')
            for _name in (f'bin/graph_watcher_{_arch}', 'bin/graph_watcher'):
                _candidate = _pkg.joinpath(_name)
                if _candidate.is_file():  # type: ignore[attr-defined]
                    _watcher_bin = str(_candidate)
                    break
        except Exception:
            _watcher_bin = None

    if _watcher_bin and sys.platform != 'linux':
        import logging
        logging.getLogger(__name__).warning(
            f"graph_watcher is a Linux binary and cannot run on {sys.platform} — "
            "graph events will not be available."
        )
        _watcher_bin = None

    if _watcher_bin:
        # Sanity-check: reject non-ELF binaries (e.g. a macOS Mach-O that
        # accidentally ended up in the PyPI wheel) before trying to run them.
        _watcher_bin_ok = False
        try:
            with open(_watcher_bin, 'rb') as _f:
                _watcher_bin_ok = _f.read(4) == b'\x7fELF'
        except Exception:
            pass
        if not _watcher_bin_ok:
            import logging
            logging.getLogger(__name__).error(
                f"osiris_graph_watcher binary at '{_watcher_bin}' is not a Linux ELF "
                "(wrong platform — was a macOS binary published by mistake?). "
                "Graph events will not be available."
            )
            _watcher_bin = None

    if _watcher_bin:
        import stat
        os.chmod(_watcher_bin, os.stat(_watcher_bin).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        _watcher_proc = subprocess.Popen(
            [_watcher_bin],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        print(f'[osiris] graph_watcher started: {_watcher_bin} (pid={_watcher_proc.pid})', flush=True)
    else:
        import logging
        logging.getLogger(__name__).warning(
            "osiris_graph_watcher not found — graph events will not be available."
        )

    rclpy.init(args=args)
    node = WebBridge(watcher_proc=_watcher_proc)

    # Forward graph_watcher stderr to the ROS logger so crashes are visible.
    # Suppress rclcpp signal-handler lines (pure noise) and fall back to plain
    # stderr once rclpy is shutting down to avoid publishing on an invalid context.
    if _watcher_proc and _watcher_proc.stderr:
        def _forward_watcher_stderr():
            ros_log = node.get_logger()
            for line in _watcher_proc.stderr:
                decoded = line.decode().rstrip()
                if not decoded:
                    continue
                # rclcpp prints these on SIGINT/SIGTERM — informational noise.
                if 'signal_handler(' in decoded:
                    continue
                if rclpy.ok():
                    try:
                        ros_log.info(f'[gw] {decoded}')
                        continue
                    except Exception:
                        pass
                print(f'[gw] {decoded}', file=sys.stderr, flush=True)
        threading.Thread(target=_forward_watcher_stderr, daemon=True).start()

    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, rclpy.executors.ExternalShutdownException):
        pass
    finally:
        # Stop the C++ watcher first so its stderr pipe closes and the
        # forwarder thread exits before we tear down the rclpy context.
        if _watcher_proc is not None:
            try:
                _watcher_proc.terminate()
                _watcher_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                _watcher_proc.kill()
                try:
                    _watcher_proc.wait(timeout=2)
                except Exception:
                    pass
            except Exception:
                pass
        try:
            node.destroy_node()
        except Exception:
            pass
        if rclpy.ok():
            try:
                rclpy.shutdown()
            except Exception:
                pass


if __name__ == '__main__':
    main()
