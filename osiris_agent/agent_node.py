import asyncio
import os
import random
import threading
import time

import rclpy
import websockets
import json

from rcl_interfaces.srv import GetParameters, ListParameters
from rclpy.node import Node
from rclpy.parameter import parameter_value_to_python

from osiris_agent import __version__ as AGENT_VERSION

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
GRAPH_CHECK_INTERVAL    = 2.0   # seconds between graph polls
TOPIC_BATCH_SIZE        = 10    # max topics enriched per tick
RECONNECT_INITIAL_DELAY = 1     # seconds
RECONNECT_MAX_DELAY     = 30    # seconds

# Services to suppress from graph output (internal ROS2 plumbing)
_SUPPRESSED_SERVICE_PREFIXES = ('/ros2cli_daemon',)


class WebBridge(Node):

    def __init__(self):
        super().__init__('osiris_node')

        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")

        base_url = os.environ.get('OSIRIS_WS_URL', 'wss://osiris-gateway.fly.dev')
        self.ws_url = f'{base_url}?robot=true&token={auth_token}'
        # self.ws_url = f'ws://host.docker.internal:8080?robot=true&token={auth_token}'

        # Declare tunable parameters
        self.declare_parameter('graph_check_interval', GRAPH_CHECK_INTERVAL)
        self.declare_parameter('topic_batch_size',     TOPIC_BATCH_SIZE)

        self.ws   = None
        self.loop = None
        self._send_queue: asyncio.Queue | None = None

        # ── Existence caches (set of fully-qualified names) ───────────────────
        self._active_nodes:    set[str] = set()
        self._active_topics:   set[str] = set()
        self._active_services: dict[str, str] = {}
        self._active_actions:  set[str] = set()

        # ── Relation caches (populated by enrichment) ─────────────────────────
        self._topic_relations: dict[str, dict] = {}

        # ── Parameters (lazy-loaded, async) ──────────────────────────────────
        self._node_parameter_cache: dict[str, dict] = {}
        self._pending_param_fetches: set[str] = set()
        self._graph_dirty = False

        # ── First-tick gate ───────────────────────────────────────────────────
        self._first_graph_check_done = False

        # ── Timers ────────────────────────────────────────────────────────────
        _graph_interval = self.get_parameter('graph_check_interval').get_parameter_value().double_value
        self._topic_batch_size = self.get_parameter('topic_batch_size').get_parameter_value().integer_value
        self.create_timer(_graph_interval, self._check_graph_changes)

        threading.Thread(target=self._run_ws_client, daemon=True).start()

        self.get_logger().info(
            f"🚀 Osiris agent v{AGENT_VERSION} — bisect 4b: first-tick scan + param fetch enabled"
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

    async def _send_initial_state(self):
        await self._send_queue.put(json.dumps({
            'type': 'agent_version',
            'version': AGENT_VERSION,
        }))

        await self._send_queue.put(json.dumps({
            'type': 'initial_state',
            'timestamp': time.time(),
            'data': {
                'nodes':       {},
                'topics':      {},
                'actions':     {},
                'services':    {},
                'telemetry':   None,
                'controllers': None,
                'hardware':    None,
                'tf_tree':     None,
            },
        }))
        self.get_logger().info("Sent initial_state (empty — bisect mode)")

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

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _node_full_name(name: str, namespace: str) -> str:
        ns = namespace if namespace.endswith('/') else namespace + '/'
        return ns + name

    # ──────────────────────────────────────────────
    # Cleanup
    # ──────────────────────────────────────────────

    def destroy_node(self):
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
