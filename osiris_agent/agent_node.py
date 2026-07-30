import asyncio
import http.client
import os
import platform
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import zipfile
from collections import deque
from pathlib import Path

import psutil
import rclpy
import websockets
import json

from rcl_interfaces.msg import ParameterEvent
from rcl_interfaces.srv import GetParameters, ListParameters
from rclpy.node import Node
from std_msgs.msg import Empty as EmptyMsg
from rclpy.parameter import Parameter, parameter_value_to_python
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
TELEMETRY_INTERVAL         = 1.0   # seconds between telemetry samples
MAX_TELEMETRY_PROCESSES    = 15    # cap on processes reported per telemetry sample
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

        # Names explicitly set via --params-file/CLI at launch — distinct
        # from self.get_parameter(name), which can't tell "the user
        # explicitly passed this" apart from "nothing was passed, this is
        # just the hardcoded declare_parameter default." _apply_agent_config
        # needs that distinction to give a deliberate local override
        # precedence over the cloud agent_config (see _resolve_config_value).
        # Node.get_parameter_overrides() isn't available on this rclpy
        # version — self._parameter_overrides is the underlying dict rclpy
        # itself populates from --params-file/CLI before any
        # declare_parameter() call consumes it, and is what that method
        # would have wrapped anyway.
        self._param_overrides = set(self._parameter_overrides.keys())

        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")

        # Declare tunable parameters
        self.declare_parameter('telemetry_enabled',      True)
        self.declare_parameter('goals_enabled',          True)
        self.declare_parameter('params_enabled',         True)
        self.declare_parameter('tf_tree_enabled',        False)
        self.declare_parameter('ros2_control_enabled',        False)
        self.declare_parameter('ros2_control_poll_interval',    2.0)
        self.declare_parameter('battery_topic',          '/battery_state')
        # Replaces the old bt_collector_enabled boolean — Nav2 BT and BT.CPP
        # share a single event pipeline (_on_bt_event/_cached_bt_tree_event,
        # no source tagging), so both being active at once would already
        # corrupt each other's state. One three-way setting makes that
        # mutual exclusion structural instead of a UI convention to enforce.
        # Default 'nav2' matches today's actual behavior (Nav2 BT always-on,
        # BT.CPP off).
        self.declare_parameter('bt_mode',               'nav2')  # 'off' | 'nav2' | 'btcpp'
        self.declare_parameter('bt_host',               '127.0.0.1')
        self.declare_parameter('bt_server_port',         1667)
        self.declare_parameter('bt_publisher_port',      1668)
        self.declare_parameter('tf_tree_poll_interval',   0.2)
        self.declare_parameter('graph_debounce_interval',   1.0)
        self.declare_parameter('bag_output_dir',            '~/ros2_bags')

        base_url = os.environ.get('OSIRIS_WS_URL', 'wss://osiris-gateway.fly.dev')
        self.ws_url = f'{base_url}?robot=true&token={auth_token}'
        # self.ws_url = f'ws://host.docker.internal:8080?robot=true&token={auth_token}'

        self.ws   = None
        self.loop = None
        self._send_queue: asyncio.Queue | None = None

        # ── Bag recording ─────────────────────────────────────────────────────
        self._bag_proc: subprocess.Popen | None = None
        self._bag_output_path: str | None = None
        self._bag_lock = threading.Lock()

        # ── Topic subscriptions (gateway-requested) ──────────────────────────
        self._topic_subs: dict[str, rclpy.subscription.Subscription] = {}
        self._topic_subs_lock = threading.Lock()
        # Rolling window of receipt timestamps per subscribed topic, used to
        # recompute each topic's rate_hz on a 1Hz timer (see _publish_topic_rates)
        # rather than piggybacking a value on topic_data itself — a piggybacked
        # rate only updates when a new message arrives, so it freezes at its last
        # value instead of decaying toward zero once a topic goes quiet.
        self._topic_rate_timestamps: dict[str, deque] = {}
        self._topic_rate_lock = threading.Lock()
        self._RATE_WINDOW_S = 5.0

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
        # None = not yet resolved by _apply_agent_config — the graph scan
        # still runs and populates _active_actions (Graph's action listing
        # needs that regardless), but _subscribe_action_status is skipped
        # until this becomes True, same "don't do the work just to undo it"
        # reasoning as TF tree. Once resolved, toggling calls _subscribe_
        # action_status/_unsubscribe_action_monitoring for every action
        # already known in _active_actions to catch up/tear down.
        self._goals_enabled_default = self.get_parameter('goals_enabled').get_parameter_value().bool_value
        self._goals_enabled = None

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
        # None = not yet resolved by _apply_agent_config. Requires graph_enabled
        # in spirit — new nodes' params are only ever discovered via the graph
        # tick, so if the reactive tick were ever disabled this would just
        # cover whatever nodes existed at first tick (not built yet, see
        # earlier discussion — no separate graph toggle exists today).
        self._params_enabled_default = self.get_parameter('params_enabled').get_parameter_value().bool_value
        self._params_enabled = None

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
        self._graph_check_pending = False  # set when a trigger arrives while a check is already running

        # ── Service scan throttle ─────────────────────────────────────────────
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
        # Starts at the local ROS param (yaml value if the user set one, else
        # its declared default of True) — overridden by the gateway's
        # agent_config push once connected, see _apply_agent_config. If that
        # push never arrives (offline, gateway didn't have a config saved,
        # etc.) this local value is simply never overwritten, so the yaml/
        # default value stands. The timer itself always runs; this just gates
        # whether each tick actually sends anything, so toggling it takes
        # effect on the very next tick with no timer start/stop bookkeeping.
        self._telemetry_enabled_default = self.get_parameter('telemetry_enabled').get_parameter_value().bool_value
        self._telemetry_enabled = self._telemetry_enabled_default

        # ── Collectors ────────────────────────────────────────────────────────
        # Not constructed here — same reasoning as TF Tree below: building a
        # Ros2ControlCollector now from the local param and possibly tearing
        # it straight back down once agent_config arrives would be wasted
        # work. Left as None until _apply_agent_config resolves the final
        # answer (config override, else this local default) and constructs
        # it at most once. self._ros2_control is already checked for None
        # everywhere it's read, so nothing else needs to change.
        self._ros2_control_enabled_default = self.get_parameter('ros2_control_enabled').get_parameter_value().bool_value
        self._ros2_control_poll_interval_default = self.get_parameter('ros2_control_poll_interval').get_parameter_value().double_value
        self._ros2_control = None
        self._ros2_control_poll_interval = None
        # Not constructed here — TfTreeCollector isn't free (real TF buffer/
        # listener), so building it now from the local param and possibly
        # tearing it straight down once agent_config arrives would be wasted
        # work. Left as None until _apply_agent_config resolves the final
        # answer (config override, else this local default) and constructs
        # it at most once. self._tf_tree is already checked for None
        # everywhere it's read, so nothing else needs to change.
        self._tf_tree_enabled_default = self.get_parameter('tf_tree_enabled').get_parameter_value().bool_value
        self._tf_tree = None

        # ── Timers ────────────────────────────────────────────────────────────
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
        self.create_timer(TELEMETRY_INTERVAL,          self._collect_telemetry)
        self.create_timer(1.0,                         self._publish_topic_rates)
        # Not created here — its period is a constructor arg to create_timer,
        # so like TF Tree/ros2_control above it's deferred to
        # _apply_agent_config, which resolves config override vs. this local
        # default and creates the timer at most once per resolved interval.
        self._tf_tree_poll_interval_default = self.get_parameter('tf_tree_poll_interval').get_parameter_value().double_value
        self._tf_tree_poll_timer = None
        self._tf_tree_poll_interval = None

        # One-time safety net: if the gateway never delivers agent_config (no
        # network, gateway down, whatever) within a few seconds of startup,
        # resolve every toggle to its local param default rather than leaving
        # things like TF tree stuck at None forever. Not needed on later
        # reconnects — once a real config has arrived once, this is a no-op.
        # Only relevant when actually waiting on the cloud in the first
        # place — a yaml params file means there's nothing to wait for (see
        # the immediate _apply_agent_config({}) call at the end of __init__),
        # so no fallback timer is created in that case either.
        self._agent_config_received = False
        self._agent_config_fallback_timer = None if self._param_overrides else self.create_timer(5.0, self._apply_agent_config_fallback)

        # ── Battery state subscription ────────────────────────────────────────
        # Not subscribed here — the topic name is a constructor arg to
        # create_subscription, so like the timers above it's deferred to
        # _apply_agent_config, which resolves config override vs. this local
        # default and (re)subscribes at most once per resolved topic.
        self._battery_topic_default = self.get_parameter('battery_topic').get_parameter_value().string_value
        self._battery_sub = None
        self._battery_topic = None

        # ── WebSocket thread ──────────────────────────────────────────────────
        threading.Thread(target=self._run_ws_client, daemon=True).start()

        # ── Optional BT collectors ────────────────────────────────────────────
        # Neither constructed here — resolved once by _apply_agent_config
        # (config override, else these local defaults), same deferred
        # reasoning as TF tree/goals/params. self._bt_mode = None means "not
        # yet resolved"; self._nav2_bt_monitor_initialized replaces the old
        # hasattr(self, '_nav2_bt_tree_id')-as-a-proxy checks throughout the
        # file, now that construction is no longer guaranteed to have
        # happened by the time any of that code runs.
        self._bt_mode_default = self.get_parameter('bt_mode').get_parameter_value().string_value
        self._bt_host_default = self.get_parameter('bt_host').get_parameter_value().string_value
        self._bt_server_port_default = self.get_parameter('bt_server_port').get_parameter_value().integer_value
        self._bt_publisher_port_default = self.get_parameter('bt_publisher_port').get_parameter_value().integer_value
        self._bt_mode = None
        self._bt_collector = None
        self._bt_collector_conn = None  # (host, server_port, publisher_port) BTCollector is currently connected with
        self._nav2_bt_monitor_initialized = False

        # A yaml params file means the operator already fully specified how
        # this run should behave — there's nothing to wait on the cloud for,
        # so resolve and construct everything right now instead of leaving
        # every feature at None until a WS connection (or the 5s fallback
        # timer, not even created in this case) gets around to it. Passing
        # {} as config makes _resolve_config_value fall through to each
        # field's local_default unconditionally, same as it always would
        # once self._param_overrides is non-empty.
        if self._param_overrides:
            self._apply_agent_config({})

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
            if msg_type == 'agent_config':
                self._apply_agent_config(data.get('config') or {})
            elif msg_type == 'subscribe':
                topic = data.get('topic')
                if topic:
                    self._subscribe_to_topic(topic)
            elif msg_type == 'unsubscribe':
                topic = data.get('topic')
                if topic:
                    self._unsubscribe_from_topic(topic)
            elif msg_type == 'error':
                self.get_logger().warning(f"Gateway error: {data.get('message', '')}")
            elif msg_type == 'bag_start_record':
                topics = data.get('topics', [])
                self.get_logger().info(
                    f"[bag] bag_start_record received  topics={topics or 'all'}"
                )
                self._start_bag_recording(data)
            elif msg_type == 'bag_stop_record':
                self.get_logger().info("[bag] bag_stop_record received")
                await asyncio.to_thread(self._stop_bag_recording)
            elif msg_type == 'bag_download_request':
                path       = data.get('path', '')
                request_id = data.get('request_id', '')
                upload_url = data.get('upload_url', '')
                self.get_logger().info(
                    f"[bag] bag_download_request received  path={path}  request_id={request_id}"
                )
                asyncio.ensure_future(
                    asyncio.to_thread(self._send_bag_download, path, request_id, upload_url)
                )

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

        nodes, topics, actions, services = await asyncio.to_thread(self._get_graph_snapshot_locked)
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
        await self._send_bag_files()

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

    async def _send_subscribe_failed(self, topic_name: str, reason: str):
        await self._send_queue.put(json.dumps({
            'type': 'subscribe_failed',
            'topic': topic_name,
            'reason': reason,
            'timestamp': time.time(),
        }))

    # ──────────────────────────────────────────────
    # Tier-1: cheap existence detection
    # ──────────────────────────────────────────────

    def _check_graph_changes(self):
        if not self._graph_check_lock.acquire(blocking=False):
            # A check is already running (e.g. slow introspection during
            # heavy graph churn). Whatever triggered this call must not be
            # silently dropped — flag it so the in-flight run loops back
            # around and re-checks before releasing the lock. This keeps
            # the pipeline purely event-driven (no periodic polling) while
            # guaranteeing every trigger is eventually acted on.
            self._graph_check_pending = True
            return
        try:
            self._check_graph_changes_locked()
            while self._graph_check_pending:
                self._graph_check_pending = False
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

        # ── 1b. Service scan ─── on node changes and follow-up ticks only ────────
        _nodes_stopped  = self._first_graph_check_done and bool(self._active_nodes - current_nodes)
        _nodes_started  = self._first_graph_check_done and bool(current_nodes - self._active_nodes)
        _do_service_scan = (
            not self._first_graph_check_done
            or _nodes_stopped
            or _nodes_started
            or self._service_rescan_ticks > 0
        )
        if _do_service_scan:
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
                    if self._params_enabled:
                        self._fetch_node_parameters_async(fqn)
                    self._fetch_lifecycle_state_async(fqn)
            self._param_fetch_timer = self.create_timer(5.0, lambda: (self._cancel_param_fetch_timer(), _fetch_all_params_delayed()))
            for _t in current_topics:
                if _t.endswith('/transition_event'):
                    self._subscribe_lifecycle_topic(_t)
                    self._fetch_lifecycle_state_async(_t[:-len('/transition_event')])
            # Resolve action types for all actions at startup — Graph's action
            # listing needs this regardless of goals_enabled. Status
            # subscription (actual goal tracking) is gated separately; see
            # self._goals_enabled.
            for a in current_actions:
                self._fetch_action_types(a, topic_type_map)
                if self._goals_enabled:
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
            if self._params_enabled:
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
            if t == '/behavior_tree_log' and self._nav2_bt_monitor_initialized:
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
            if t == '/behavior_tree_log' and self._nav2_bt_monitor_initialized:
                self._on_nav2_bt_gone()

        # ── 4. Service changes ─────────────────────────────────────────────────────────────
        if _do_service_scan:
            if set(current_services) != set(self._active_services):
                self._graph_dirty = True

        # ── 5. Action events ──────────────────────────────────────────────────
        for a in current_actions - self._active_actions:
            self._fetch_action_types(a, topic_type_map)
            if self._goals_enabled:
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
        if self._nav2_bt_monitor_initialized:
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
                if topic == '/behavior_tree_log' and self._nav2_bt_monitor_initialized:
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

    def _get_graph_snapshot_locked(self) -> tuple[dict, dict, dict, dict]:
        """Gather all four graph relation dicts under _graph_check_lock.

        _check_graph_changes_locked() mutates _topic_relations / _active_*
        in place on the timer/executor thread; without this lock a caller
        on another thread (e.g. _send_initial_state on the websocket thread)
        can observe a torn dict mid-mutation (RuntimeError: dictionary
        changed size during iteration, or a silently incomplete snapshot).
        """
        with self._graph_check_lock:
            return (
                self._get_nodes_with_relations(),
                self._get_topics_with_relations(),
                self._get_actions_with_relations(),
                self._get_services_with_relations(),
            )

    # ──────────────────────────────────────────────
    # Delta-send: flush graph snapshots after each tick
    # ──────────────────────────────────────────────

    def _flush_graph_snapshots(self):
        if not self._graph_dirty or not self.ws or not self.loop:
            return

        # Only clear the dirty flag once every snapshot has actually been
        # built and enqueued. If anything below raises, the flag is put
        # back so the next trigger retries instead of the client silently
        # never receiving this update.
        try:
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
        except Exception:
            self._graph_dirty = True
            raise
        else:
            self._graph_dirty = False

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
                if self.loop:
                    asyncio.run_coroutine_threadsafe(
                        self._send_subscribe_failed(topic_name, 'subscription_limit_reached'), self.loop
                    )
                return

        types = dict(self.get_topic_names_and_types()).get(topic_name)
        if not types:
            self.get_logger().warning(f"Topic not found: {topic_name}")
            if self.loop:
                asyncio.run_coroutine_threadsafe(
                    self._send_subscribe_failed(topic_name, 'topic_not_found'), self.loop
                )
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

    # ── Bag recording ──────────────────────────────────────────────────────────

    def _get_bag_files_snapshot(self) -> list[dict]:
        """Return metadata for every completed bag in the output directory."""
        bag_dir = os.path.expanduser(
            self.get_parameter('bag_output_dir').get_parameter_value().string_value
        )
        bags = []
        try:
            entries = sorted(Path(bag_dir).iterdir())
        except (FileNotFoundError, OSError):
            return bags
        for entry in entries:
            if not entry.is_dir():
                continue
            # Skip the directory that is currently being recorded.
            with self._bag_lock:
                if str(entry) == self._bag_output_path:
                    continue
            try:
                size_bytes = sum(
                    f.stat().st_size for f in entry.rglob('*') if f.is_file()
                )
            except OSError:
                size_bytes = 0
            # Prefer the timestamp embedded in the directory name (bag_<ts>),
            # fall back to the directory mtime.
            name = entry.name
            try:
                created_at = float(name.split('_', 1)[1])
            except (IndexError, ValueError):
                created_at = entry.stat().st_mtime
            bags.append({
                'name':       name,
                'path':       str(entry),
                'size_bytes': size_bytes,
                'created_at': created_at,
            })
        return bags

    async def _send_bag_files(self):
        bags = await asyncio.to_thread(self._get_bag_files_snapshot)
        await self._send_queue.put(json.dumps({
            'type':      'bag_files',
            'bags':      bags,
            'timestamp': time.time(),
        }))
        self.get_logger().info(f"[bag] bag_files sent to gateway  count={len(bags)}")

    def _send_bag_download(self, path: str, request_id: str, upload_url: str):
        """Zip the bag directory and POST it to the gateway upload endpoint."""

        # ── Path traversal guard ──────────────────────────────────────────────
        bag_dir = os.path.expanduser(
            self.get_parameter('bag_output_dir').get_parameter_value().string_value
        )
        bag_dir_real = os.path.realpath(bag_dir)
        path_real    = os.path.realpath(path)
        if not (path_real == bag_dir_real or
                path_real.startswith(bag_dir_real + os.sep)):
            self.get_logger().error(
                f"[bag] download rejected — path outside bag_output_dir: {path}"
            )
            return

        if not os.path.isdir(path_real):
            self.get_logger().error(
                f"[bag] download rejected — not a directory: {path}"
            )
            return

        tmp_path = None
        try:
            name = os.path.basename(path_real)
            self.get_logger().info(
                f"[bag] zipping {path_real}  request_id={request_id}"
            )

            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_f:
                tmp_path = tmp_f.name

            with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for file in sorted(Path(path_real).rglob('*')):
                    if file.is_file():
                        zf.write(file, file.relative_to(Path(path_real).parent))

            zip_size = os.path.getsize(tmp_path)
            self.get_logger().info(
                f"[bag] zip ready  name={name}.zip  size={zip_size} bytes  "
                f"uploading to {upload_url}"
            )

            # ── Stream-upload via stdlib http.client (no extra dependencies) ───
            # Derive host/scheme from the active WS URL so the upload reaches
            # the same endpoint even when the gateway sends 'localhost' (which
            # would resolve to the container itself, not the host).
            ws_parsed     = urllib.parse.urlparse(self.ws_url)
            http_scheme   = 'https' if ws_parsed.scheme == 'wss' else 'http'
            netloc        = ws_parsed.netloc.split('?')[0]  # strip any query fragment
            upload_path   = urllib.parse.urlparse(upload_url).path
            qs            = urllib.parse.urlencode({'request_id': request_id})
            path_q        = f"{upload_path}?{qs}"

            self.get_logger().info(
                f"[bag] effective upload target: {http_scheme}://{netloc}{path_q}"
            )

            conn = (
                http.client.HTTPSConnection(netloc, timeout=120)
                if http_scheme == 'https'
                else http.client.HTTPConnection(netloc, timeout=120)
            )
            try:
                with open(tmp_path, 'rb') as f:
                    conn.request(
                        'POST', path_q, body=f,
                        headers={
                            'Content-Type':   'application/zip',
                            'Content-Length': str(zip_size),
                        },
                    )
                resp = conn.getresponse()
                resp.read()  # drain so the connection can be reused / closed cleanly
                self.get_logger().info(
                    f"[bag] upload complete  status={resp.status}  "
                    f"request_id={request_id}"
                )
            finally:
                conn.close()

        except Exception as e:
            self.get_logger().error(
                f"[bag] download upload failed  request_id={request_id}: {e}"
            )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                    self.get_logger().debug(f"[bag] temp zip deleted: {tmp_path}")
                except OSError:
                    pass

    def _start_bag_recording(self, data: dict):
        with self._bag_lock:
            if self._bag_proc is not None and self._bag_proc.poll() is None:
                self.get_logger().warning(
                    "[bag] rejected start — recording already in progress"
                )
                self._enqueue({
                    'type': 'error',
                    'message': 'Bag recording already in progress',
                })
                return

            topics: list[str] = data.get('topics', [])
            bag_dir = os.path.expanduser(
                self.get_parameter('bag_output_dir').get_parameter_value().string_value
            )
            os.makedirs(bag_dir, exist_ok=True)
            output_path = os.path.join(bag_dir, f'bag_{int(time.time())}')

            if topics:
                cmd = ['ros2', 'bag', 'record', '-o', output_path] + topics
            else:
                cmd = ['ros2', 'bag', 'record', '-a', '-o', output_path]

            self.get_logger().info(
                f"[bag] launching subprocess: {' '.join(cmd)}"
            )
            self._bag_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            self._bag_output_path = output_path
            self.get_logger().info(
                f"[bag] subprocess started  pid={self._bag_proc.pid}  output={output_path}"
            )

        self._enqueue({
            'type':      'bag_record_started',
            'path':      output_path,
            'topics':    topics if topics else 'all',
            'timestamp': time.time(),
        })
        self.get_logger().info(
            f"[bag] bag_record_started sent to gateway  topics={topics or 'all'}"
        )

    def _stop_bag_recording(self):
        with self._bag_lock:
            if self._bag_proc is None or self._bag_proc.poll() is not None:
                self.get_logger().warning(
                    "[bag] rejected stop — no recording is currently in progress"
                )
                self._enqueue({
                    'type':    'error',
                    'message': 'No bag recording is currently in progress',
                })
                return

            proc = self._bag_proc
            output_path = self._bag_output_path
            self._bag_proc = None
            self._bag_output_path = None

        # SIGINT lets ros2 bag flush the SQLite index before exiting.
        self.get_logger().info(
            f"[bag] sending SIGINT to pid={proc.pid}  output={output_path}"
        )
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=10)
            self.get_logger().info("[bag] subprocess exited cleanly")
        except subprocess.TimeoutExpired:
            self.get_logger().warning(
                "[bag] subprocess did not exit within 10 s — sending SIGKILL"
            )
            proc.kill()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass

        size_bytes = 0
        try:
            size_bytes = sum(
                f.stat().st_size
                for f in Path(output_path).rglob('*')
                if f.is_file()
            )
        except (FileNotFoundError, OSError):
            pass

        name = os.path.basename(output_path)
        self.get_logger().info(
            f"[bag] recording finalised  name={name}  size={size_bytes} bytes"
        )
        self._enqueue({
            'type':       'bag_record_stopped',
            'name':       name,
            'path':       output_path,
            'size_bytes': size_bytes,
            'timestamp':  time.time(),
        })
        self.get_logger().info(
            f"[bag] bag_record_stopped sent to gateway  size={size_bytes} bytes"
        )

    def _on_topic_msg(self, msg, topic_name: str):
        if not self.ws or not self.loop:
            return

        ts = time.time()
        with self._topic_rate_lock:
            self._topic_rate_timestamps.setdefault(topic_name, deque()).append(ts)

        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps({
                'type': 'topic_data',
                'topic': topic_name,
                'data': message_to_ordereddict(msg),
                'timestamp': ts,
            })),
            self.loop,
        )

    def _publish_topic_rates(self):
        """Periodic 1Hz timer callback — recomputes every subscribed topic's
        rate_hz from a rolling window of receipt timestamps and pushes it as its
        own message, independent of whether that topic published anything this
        tick. This is what makes a quiet topic's rate correctly decay to 0
        instead of freezing at its last computed value."""
        if not self.ws or not self.loop:
            return

        with self._topic_subs_lock:
            subscribed = list(self._topic_subs.keys())

        now = time.time()
        cutoff = now - self._RATE_WINDOW_S
        rates = {}
        with self._topic_rate_lock:
            for topic in subscribed:
                buf = self._topic_rate_timestamps.get(topic)
                if buf:
                    while buf and buf[0] < cutoff:
                        buf.popleft()
                    rates[topic] = round(len(buf) / self._RATE_WINDOW_S, 2)
                else:
                    rates[topic] = 0.0
            # Drop buffers for topics no longer subscribed so this dict doesn't
            # grow unbounded across repeated subscribe/unsubscribe cycles.
            for stale in set(self._topic_rate_timestamps) - set(subscribed):
                del self._topic_rate_timestamps[stale]

        self._enqueue({
            'type': 'topic_rates',
            'rates': rates,
            'timestamp': now,
        })

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

    def _catch_up_action_feedback(self, action_name: str):
        """Subscribes to feedback for an action whose type was already
        resolved (cached) while goals_enabled was False. _fetch_action_types
        early-returns once an action is cached, so simply re-calling it here
        would silently no-op instead of subscribing — this reuses the
        cached _feedback_msg_type instead of redoing type resolution."""
        type_info = self._action_type_cache.get(action_name)
        if not type_info:
            return
        try:
            feedback_msg_cls = get_message(type_info['_feedback_msg_type'])
            self._subscribe_action_feedback(action_name, feedback_msg_cls)
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to catch up feedback subscription for {action_name}: {e}')

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
        self.get_logger().debug("[graph] event received")
        self._trigger_graph_poll()

    def _trigger_graph_poll(self):
        """Single debounced entry point for all graph poll triggers.

        Resets a one-shot timer on every call so rapid bursts coalesce
        into a single poll.
        """
        if self._graph_debounce_timer is not None:
            self._graph_debounce_timer.cancel()
        _interval = self.get_parameter('graph_debounce_interval').get_parameter_value().double_value
        self._graph_debounce_timer = threading.Timer(_interval, self._debounce_fire)
        self._graph_debounce_timer.daemon = True
        self._graph_debounce_timer.start()

    def _debounce_fire(self):
        """Called from threading.Timer — run the graph poll directly."""
        self.get_logger().debug("[graph] watcher triggered poll")
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
                # Wire message type (not the same string as feedback_type
                # above — that's the nested class name, this is the
                # standalone _FeedbackMessage actually subscribed to) kept
                # so a later goals_enabled toggle-on can catch up the
                # feedback subscription without redoing type resolution —
                # see _catch_up_action_feedback.
                '_feedback_msg_type': feedback_msg_type,
            }
            self._action_type_cache[action_name] = type_info
            self.get_logger().info(f'[actions] resolved types for {action_name}: {base_type}')
            self._graph_dirty = True
            if self._goals_enabled:
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

    def _apply_agent_config_fallback(self):
        self._agent_config_fallback_timer.cancel()
        if self._agent_config_received:
            return
        self.get_logger().warning('No agent_config received within timeout — applying local param defaults')
        self._apply_agent_config({})

    def _resolve_config_value(self, name, config, local_default, cast=None):
        """All-or-nothing per agent run, not a per-field override: passing
        --params-file at all is a deliberate choice to run off that yaml
        file, so every field resolves from it (yaml value if the file sets
        this one, else its hardcoded declare_parameter default) and the
        cloud agent_config is ignored entirely for the whole run — not just
        for the specific fields the yaml file happens to set. Without a
        params file, every field resolves from the cloud config if present,
        else the hardcoded default. self._param_overrides being non-empty at
        all (regardless of which names it contains) is what decides which
        source every field uses — the two sources never mix within a run.
        """
        if self._param_overrides:
            return local_default
        if name in config:
            return cast(config[name]) if cast else config[name]
        return local_default

    def _apply_agent_config(self, config: dict) -> None:
        """Applies the gateway-pushed feature-toggle config (sent right after
        auth_success on every connect, and by _apply_agent_config_fallback if
        that never arrives). Per-field precedence: local yaml/CLI override >
        cloud agent_config > hardcoded default — see _resolve_config_value.
        A robot with nothing set in agent_config yet, and no yaml override
        either, behaves exactly as it did before this existed. Single
        dispatch point so each new toggle (Graph, Params, Nav2, Goals, BT,
        TF Tree) has one place to land rather than scattering config reads
        across the file.
        """
        self._agent_config_received = True

        self._telemetry_enabled = self._resolve_config_value('telemetry_enabled', config, self._telemetry_enabled_default, bool)

        # TF tree: resolve the final answer, then construct/destroy the
        # collector at most once to reach it — never both in the same pass.
        tf_tree_enabled = self._resolve_config_value('tf_tree_enabled', config, self._tf_tree_enabled_default, bool)
        if tf_tree_enabled and self._tf_tree is None:
            self._tf_tree = TfTreeCollector(
                node=self,
                event_callback=self._on_tf_tree_event,
                logger=self.get_logger(),
            )
            self.get_logger().info('TF tree monitoring started')
        elif not tf_tree_enabled and self._tf_tree is not None:
            self._tf_tree.destroy()
            self._tf_tree = None
            self.get_logger().info('TF tree monitoring stopped')

        # TF tree poll timer: independent of the collector above — _poll_tf_tree
        # is a no-op whenever self._tf_tree is None, so the timer's period is
        # resolved and (re)created here regardless of tf_tree_enabled, exactly
        # like telemetry's own timer runs unconditionally.
        tf_tree_poll_interval = self._resolve_config_value('tf_tree_poll_interval', config, self._tf_tree_poll_interval_default, float)
        if self._tf_tree_poll_timer is None or self._tf_tree_poll_interval != tf_tree_poll_interval:
            if self._tf_tree_poll_timer is not None:
                self._tf_tree_poll_timer.cancel()
            self._tf_tree_poll_timer = self.create_timer(tf_tree_poll_interval, self._poll_tf_tree)
            self._tf_tree_poll_interval = tf_tree_poll_interval

        # ros2_control: same construct/destroy-at-most-once pattern as TF
        # tree, plus the same reconnect-if-changed handling as bt_conn below
        # — poll_interval is a constructor arg baked into the collector
        # (rate-limit check in Ros2ControlCollector.poll()), not re-read
        # live, so a changed interval while already enabled needs an actual
        # destroy+reconstruct to ever take effect.
        ros2_control_enabled = self._resolve_config_value('ros2_control_enabled', config, self._ros2_control_enabled_default, bool)
        ros2_control_poll_interval = self._resolve_config_value('ros2_control_poll_interval', config, self._ros2_control_poll_interval_default, float)
        if ros2_control_enabled and (self._ros2_control is None or self._ros2_control_poll_interval != ros2_control_poll_interval):
            if self._ros2_control is not None:
                self._ros2_control.destroy()
            self._ros2_control = Ros2ControlCollector(
                node=self,
                event_callback=self._on_ros2_control_event,
                logger=self.get_logger(),
                poll_interval=ros2_control_poll_interval,
            )
            self._ros2_control_poll_interval = ros2_control_poll_interval
            self.get_logger().info('ros2_control monitoring started')
        elif not ros2_control_enabled and self._ros2_control is not None:
            self._ros2_control.destroy()
            self._ros2_control = None
            self.get_logger().info('ros2_control monitoring stopped')

        # Battery topic: the subscription's topic name is a constructor arg,
        # so it's (re)created here whenever the resolved topic changes.
        battery_topic = self._resolve_config_value('battery_topic', config, self._battery_topic_default)
        if self._battery_sub is None or self._battery_topic != battery_topic:
            if self._battery_sub is not None:
                self.destroy_subscription(self._battery_sub)
                self._battery_sub = None
            try:
                from sensor_msgs.msg import BatteryState as BatteryStateMsg
                self._battery_sub = self.create_subscription(
                    BatteryStateMsg, battery_topic,
                    self._on_battery_state, 10,
                )
                self.get_logger().info(f'Battery state subscription active on {battery_topic}')
            except Exception as e:
                self.get_logger().warning(f'Battery state monitoring unavailable: {e}')
            self._battery_topic = battery_topic

        # Goals: no single collector object to construct/destroy — just a set
        # of per-action subscriptions. Turning on catches up on every action
        # already known (self._active_actions, populated by the graph scan
        # regardless of this flag); turning off tears all of them down.
        goals_enabled = self._resolve_config_value('goals_enabled', config, self._goals_enabled_default, bool)
        if goals_enabled and not self._goals_enabled:
            for a in self._active_actions:
                self._subscribe_action_status(a)
                self._catch_up_action_feedback(a)
            self.get_logger().info(f'Goal tracking started ({len(self._active_actions)} action(s))')
        elif not goals_enabled and self._goals_enabled:
            for a in list(set(self._action_status_subs) | set(self._action_feedback_subs)):
                self._unsubscribe_action_monitoring(a)
            self.get_logger().info('Goal tracking stopped')
        self._goals_enabled = goals_enabled

        # Params: no collector/subscription to construct/destroy either — just
        # a cache. Turning on catches up on every node already known
        # (self._active_nodes, populated by the graph scan regardless of this
        # flag); turning off clears the cache so stale values don't keep
        # showing in the Params pane after the user asked this to stop.
        params_enabled = self._resolve_config_value('params_enabled', config, self._params_enabled_default, bool)
        if params_enabled and not self._params_enabled:
            for fqn in self._active_nodes:
                self._fetch_node_parameters_async(fqn)
            self.get_logger().info(f'Param fetching started ({len(self._active_nodes)} node(s))')
        elif not params_enabled and self._params_enabled:
            self._node_parameter_cache.clear()
            self._pending_param_fetches.clear()
            self._graph_dirty = True
            self.get_logger().info('Param fetching stopped')
        self._params_enabled = params_enabled

        # BT: mutually exclusive by construction — Nav2 BT and BT.CPP share
        # the same event pipeline (_on_bt_event/_cached_bt_tree_event, no
        # source tagging), so only one may ever be active. Tear down
        # whichever isn't the resolved mode before starting the other, so a
        # switch never briefly has both running. BT.CPP additionally
        # reconnects if its host/port changed while already in btcpp mode —
        # a running BTCollector is a live ZMQ connection bound to whatever
        # host/port it was constructed with.
        bt_mode = self._resolve_config_value('bt_mode', config, self._bt_mode_default)
        bt_host = self._resolve_config_value('bt_host', config, self._bt_host_default)
        bt_server_port = self._resolve_config_value('bt_server_port', config, self._bt_server_port_default, int)
        bt_publisher_port = self._resolve_config_value('bt_publisher_port', config, self._bt_publisher_port_default, int)
        bt_conn = (bt_host, bt_server_port, bt_publisher_port)

        if bt_mode != 'nav2' and self._nav2_bt_monitor_initialized:
            self._teardown_nav2_bt_monitor()
        if bt_mode != 'btcpp' and self._bt_collector is not None:
            self._bt_collector.stop()
            self._bt_collector = None
            self._bt_collector_conn = None

        if bt_mode == 'nav2' and not self._nav2_bt_monitor_initialized:
            self._init_nav2_bt_monitor()
        elif bt_mode == 'btcpp' and (self._bt_collector is None or self._bt_collector_conn != bt_conn):
            if self._bt_collector is not None:
                self._bt_collector.stop()
            self._bt_collector = BTCollector(
                event_callback=self._on_bt_event,
                host=bt_host,
                server_port=bt_server_port,
                publisher_port=bt_publisher_port,
                logger=self.get_logger(),
            )
            self._bt_collector.start()
            self._bt_collector_conn = bt_conn
            self.get_logger().info(f'BT.CPP monitoring started ({bt_host}:{bt_server_port}/{bt_publisher_port})')

        self._bt_mode = bt_mode

        # bag_output_dir / graph_debounce_interval: every consumer already
        # calls self.get_parameter(...) fresh at time of use (a plain path
        # string re-read on each bag list/download/record; a plain float
        # re-read into a brand-new threading.Timer on every debounce trigger,
        # not a fixed recurring ROS timer) — so there's nothing to construct
        # or defer here, just update the underlying ROS param when overridden.
        # Same all-or-nothing rule as _resolve_config_value: a yaml params
        # file being present at all (regardless of whether it sets these two
        # specific fields) means the cloud config is skipped for both — the
        # ROS param already holds the yaml-or-hardcoded value and is simply
        # left untouched.
        if not self._param_overrides and 'bag_output_dir' in config:
            self.set_parameters([Parameter('bag_output_dir', Parameter.Type.STRING, str(config['bag_output_dir']))])
        if not self._param_overrides and 'graph_debounce_interval' in config:
            self.set_parameters([Parameter('graph_debounce_interval', Parameter.Type.DOUBLE, float(config['graph_debounce_interval']))])

        self.get_logger().info(
            f'Applied agent_config: telemetry_enabled={self._telemetry_enabled}, '
            f'tf_tree_enabled={tf_tree_enabled}, goals_enabled={goals_enabled}, '
            f'params_enabled={params_enabled}, bt_mode={bt_mode}'
        )

        # Ground truth for the client: what this agent is ACTUALLY running
        # with right now, for every field — as opposed to the cloud
        # agent_config, which can be completely irrelevant (yaml-file runs
        # ignore it outright) or simply stale until this agent reconnects.
        # Sent on every _apply_agent_config call (every connect/reconnect,
        # and the 5s fallback), so the client always has a current answer,
        # never a guess based on pane data being empty. bag_output_dir and
        # graph_debounce_interval are read fresh here rather than reusing a
        # local var — they're the two fields actually pushed through
        # set_parameters() above rather than tracked as plain instance
        # state, so this is the one place their resolved value lives.
        #
        # yaml_override is metadata about the SOURCE these values were
        # resolved from, not a config value itself — a yaml/CLI params file
        # being passed at all means the client shouldn't claim a restart
        # will sync Cloud Config (it never will, as long as that file keeps
        # getting passed); it should instead tell the operator how to
        # actually switch back to cloud-driven config.
        self._enqueue({
            'type': 'resolved_agent_config',
            'yaml_override': bool(self._param_overrides),
            'config': {
                'telemetry_enabled': self._telemetry_enabled,
                'goals_enabled': goals_enabled,
                'params_enabled': params_enabled,
                'tf_tree_enabled': tf_tree_enabled,
                'tf_tree_poll_interval': tf_tree_poll_interval,
                'ros2_control_enabled': ros2_control_enabled,
                'ros2_control_poll_interval': ros2_control_poll_interval,
                'battery_topic': battery_topic,
                'bt_mode': bt_mode,
                'bt_host': bt_host,
                'bt_server_port': bt_server_port,
                'bt_publisher_port': bt_publisher_port,
                'bag_output_dir': self.get_parameter('bag_output_dir').get_parameter_value().string_value,
                'graph_debounce_interval': self.get_parameter('graph_debounce_interval').get_parameter_value().double_value,
            },
            'timestamp': time.time(),
        })

    def _collect_telemetry(self):
        if not self.ws or not self.loop:
            return
        if not self._telemetry_enabled:
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

        # CPU frequency (GHz)
        cpu_freq = None
        try:
            freq = psutil.cpu_freq()
            if freq and freq.current:
                cpu_freq = round(freq.current / 1000.0, 2)
        except Exception:
            pass

        # Load averages (1, 5, 15 min)
        cpu_load = None
        try:
            load1, load5, load15 = os.getloadavg()
            cpu_load = {
                'load1':  round(load1, 1),
                'load5':  round(load5, 1),
                'load15': round(load15, 1),
            }
        except Exception:
            pass

        # Process list (top processes by CPU usage).
        # Two-phase fetch: cheap fields for all processes first, then only
        # pull the more expensive fields (cmdline, memory_info, username) for
        # the top N CPU consumers, to avoid per-second syscalls against every
        # process on the host and to avoid leaking the full host process list
        # (cmdline can contain secrets) over the wire.
        processes = []
        try:
            candidates = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent']):
                try:
                    info = proc.info
                    candidates.append((round(info['cpu_percent'] or 0.0, 1), proc))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            candidates.sort(key=lambda c: c[0], reverse=True)

            for cpu_percent, proc in candidates[:MAX_TELEMETRY_PROCESSES]:
                try:
                    with proc.oneshot():
                        cmdline = proc.cmdline()
                        mem_info = proc.memory_info()
                        processes.append({
                            'pid':          proc.pid,
                            'name':         proc.name(),
                            'cmdline':      ' '.join(cmdline)[:256] if cmdline else '',
                            'num_threads':  proc.num_threads(),
                            'username':     proc.username(),
                            'memory_mb':    round(mem_info.rss / (1024 * 1024), 1) if mem_info else 0,
                            'cpu_percent':  cpu_percent,
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception:
            pass

        return {
            'cpu': {
                'now':        cpu_now,
                'throttling': None,
                'temp':       cpu_c,
                'freq':       cpu_freq,
                'load':       cpu_load,
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
            'battery':   self._last_battery_state,
            'processes': processes,
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
            self._nav2_bt_log_sub = self.create_subscription(
                BehaviorTreeLog, '/behavior_tree_log', self._on_nav2_bt_log, 10
            )
            self._nav2_bt_goal_status_sub = self.create_subscription(
                GoalStatusArray,
                '/navigate_to_pose/_action/status',
                self._on_nav2_goal_status,
                10,
            )
            self._nav2_bt_monitor_initialized = True
            # If bt_navigator is already publishing, pre-parse the XML so
            # the startup bt_state event includes the tree structure.
            if self.count_publishers('/behavior_tree_log') > 0:
                self._nav2_bt_publisher_active = True
                self._load_and_parse_bt_xml()
        except Exception as e:
            self.get_logger().debug(f"Nav2 BT monitoring unavailable: {e}")

    def _teardown_nav2_bt_monitor(self):
        if not self._nav2_bt_monitor_initialized:
            return
        self._on_nav2_bt_gone()  # notifies client the tree is gone, resets tree-state fields
        self.destroy_subscription(self._nav2_bt_log_sub)
        self.destroy_subscription(self._nav2_bt_goal_status_sub)
        self._nav2_bt_log_sub = None
        self._nav2_bt_goal_status_sub = None
        self._nav2_bt_monitor_initialized = False
        self.get_logger().info('Nav2 BT monitoring stopped')

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
                'blackboard': src.get('blackboard'),
            }
        if (
            self._nav2_bt_monitor_initialized
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
                'blackboard': None,
            }
        return {
            'type': 'bt_state', 'timestamp': time.time(),
            'tree_id': None, 'tree': None, 'nodes': [], 'blackboard': None,
        }

    def _bt_snapshot_from_state_event(self, src: dict) -> dict:
        return {
            'timestamp': src.get('timestamp', time.time()),
            'tree_id': src.get('tree_id'),
            'tree': src.get('tree'),
            'nodes': src.get('nodes', []),
            'blackboard': src.get('blackboard'),
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
        with self._bag_lock:
            bag_proc = self._bag_proc
            self._bag_proc = None
            self._bag_output_path = None
        if bag_proc is not None and bag_proc.poll() is None:
            try:
                bag_proc.send_signal(signal.SIGINT)
                bag_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                bag_proc.kill()
                try:
                    bag_proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
        super().destroy_node()


def main(args=None):
    import shutil
    import subprocess
    import importlib.resources

    # Locate the graph_watcher binary:
    # 1. Prefer PATH (colcon dev workspace with source install/setup.bash)
    # 2. Fall back to distro+arch-specific binary:  bin/graph_watcher_{arch}_{distro}
    #    (e.g. graph_watcher_aarch64_lyrical, graph_watcher_x86_64_jazzy)
    # 3. Fall back to arch-only binary:             bin/graph_watcher_{arch}
    # 4. Fall back to bin/graph_watcher (legacy / colcon-installed generic name)
    _watcher_proc = None
    _watcher_bin = shutil.which('graph_watcher')
    if _watcher_bin is None:
        try:
            _arch = platform.machine()  # 'x86_64' or 'aarch64'
            _distro = os.environ.get('ROS_DISTRO', '')  # e.g. 'humble', 'jazzy', 'lyrical'
            _pkg = importlib.resources.files('osiris_agent')
            _candidates = []
            if _distro:
                _candidates.append(f'bin/graph_watcher_{_arch}_{_distro}')
            _candidates += [f'bin/graph_watcher_{_arch}', 'bin/graph_watcher']
            for _name in _candidates:
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
    