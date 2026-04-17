import asyncio
import os
import random
import threading
import time

import rclpy
import websockets
import json

from rclpy.node import Node

from osiris_agent import __version__ as AGENT_VERSION

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
RECONNECT_INITIAL_DELAY = 1     # seconds
RECONNECT_MAX_DELAY     = 30    # seconds


class WebBridge(Node):

    def __init__(self):
        super().__init__('osiris_node')

        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")

        base_url = os.environ.get('OSIRIS_WS_URL', 'wss://osiris-gateway.fly.dev')
        # self.ws_url = f'{base_url}?robot=true&token={auth_token}'
        self.ws_url = f'ws://host.docker.internal:8080?robot=true&token={auth_token}'

        self.ws   = None
        self.loop = None
        self._send_queue: asyncio.Queue | None = None

        threading.Thread(target=self._run_ws_client, daemon=True).start()

        self.get_logger().info(
            f"🚀 Osiris agent v{AGENT_VERSION} — bisect: minimal WS-only mode"
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
