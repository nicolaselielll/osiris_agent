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
MAX_SUBSCRIPTIONS          = 100   # hard cap on gateway-requested topic subs
RECONNECT_INITIAL_DELAY    = 1     # seconds
RECONNECT_MAX_DELAY        = 30    # seconds

# Services to suppress from graph output (internal ROS2 plumbing)
_SUPPRESSED_SERVICE_PREFIXES = ('/ros2cli_daemon',)


class WebBridge(Node):

    def __init__(self):
        super().__init__('osiris_node')

        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")

        # Declare tunable parameters
        self.declare_parameter('graph_check_interval', GRAPH_CHECK_INTERVAL)
        self.declare_parameter('topic_batch_size',     TOPIC_BATCH_SIZE)
        self.declare_parameter('telemetry_interval',   TELEMETRY_INTERVAL)
        self.declare_parameter('tf_tree_enabled',      False)

        base_url = os.environ.get('OSIRIS_WS_URL', 'wss://osiris-gateway.fly.dev')
        self.ws_url = f'{base_url}?robot=true&token={auth_token}'
        # self.ws_url = f'ws://host.docker.internal:8080?robot=true&token={auth_token}'

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

        # ── Count sentinels (cheap change detection) ─────────────────────────
        self._topic_counts: dict[str, tuple[int, int]] = {}  # topic → (pub_n, sub_n)

        # ── Relation caches (populated by Tier-2 enrichment) ─────────────────
        self._topic_relations: dict[str, dict] = {}

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

        # ── Service scan throttle ─────────────────────────────────────────────
        self._last_service_scan: float = 0.0
        self._service_rescan_ticks: int = 0

        # ── Initial scan synchronization ──────────────────────────────────────
        self._initial_scan_complete = threading.Event()
        self._first_graph_check_done = False

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

        # ── Timers ────────────────────────────────────────────────────────────
        _graph_interval = self.get_parameter('graph_check_interval').get_parameter_value().double_value
        self._topic_batch_size = self.get_parameter('topic_batch_size').get_parameter_value().integer_value
        self.create_timer(_graph_interval, self._check_graph_changes)
        # NOTE: _collect_telemetry and _refresh_empty_param_caches timers are
        # NOT wired yet — held for bisect step 4e (enable polling).

        # ── WebSocket thread ──────────────────────────────────────────────────
        threading.Thread(target=self._run_ws_client, daemon=True).start()

        self.get_logger().info(
            f"🚀 Osiris agent v{AGENT_VERSION} — bisect 4c: state hydration + collectors (inert)"
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
        self._ros2_control.poll()
        if self._tf_tree is not None:
            self._tf_tree.poll(force=True)
        self._initial_scan_complete.set()
        self.get_logger().info(
            f"[poll] first tick complete: {len(current_nodes)} nodes, {len(current_topics)} topics, "
            f"{len(current_services)} services, {len(current_actions)} actions — "
            f"node+topic={(_t1-_t0)*1000:.1f}ms enrichment={(_te1-_te0)*1000:.1f}ms"
        )

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

        _pending_before = len(self._pending_topic_enrichment)
        _t0 = time.time()
        batch = set(list(self._pending_topic_enrichment)[:self._topic_batch_size])
        self._pending_topic_enrichment -= batch
        self.get_logger().info(
            f"[enrich] batch={len(batch)}, pending_before={_pending_before}, "
            f"remaining={len(self._pending_topic_enrichment)}"
        )

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
                self._flush_graph_snapshots()

            get_future.add_done_callback(_on_get)

        future.add_done_callback(_on_list)

    # ──────────────────────────────────────────────
    # Telemetry
    # ──────────────────────────────────────────────

    def _collect_telemetry(self):
        if not self._telemetry_enabled or not self.ws or not self.loop:
            return
        self._enqueue({
            'type': 'telemetry',
            'data': self._get_telemetry_snapshot(),
            'timestamp': time.time(),
        })

    def _get_telemetry_snapshot(self) -> dict:
        cpu_now = round(psutil.cpu_percent(interval=None), 1)
        self._cpu_history.append(cpu_now)

        def _rolling(n: int) -> float | None:
            window = list(self._cpu_history)[-n:]
            return round(sum(window) / len(window), 1) if window else None

        load1  = _rolling(60)
        load5  = _rolling(300)
        load15 = _rolling(900)

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

    # ──────────────────────────────────────────────
    # Cleanup
    # ──────────────────────────────────────────────

    def destroy_node(self):
        self._ros2_control.destroy()
        if self._tf_tree is not None:
            self._tf_tree.destroy()
        super().destroy_node()


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
