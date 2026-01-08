# OSIRIS Agent

A ROS2 Humble node that bridges your robot to the OSIRIS remote monitoring platform via WebSocket.

## Overview

The OSIRIS Agent (`osiris_agent`) is a ROS2 node that provides real-time bidirectional communication between your ROS2 robot and a remote WebSocket gateway. It monitors the ROS2 graph, collects system telemetry, and enables dynamic topic subscription from the remote platform.

## Features

### Real-time Graph Monitoring
- **Node Detection**: Automatically detects when ROS2 nodes start/stop
- **Topic Tracking**: Monitors topic creation, destruction, and subscription changes
- **Action Discovery**: Tracks ROS2 actions and their lifecycle
- **Service Discovery**: Detects available services and their endpoints
- **Publisher/Subscriber Relations**: Maps which nodes publish to and subscribe from each topic

### Dynamic Topic Subscription
- Subscribe to any ROS2 topic remotely via WebSocket commands
- Automatic message type resolution and serialization
- Configurable subscription limits (default: 100 concurrent subscriptions)
- Thread-safe subscription management
- Topic publishing rate calculation

### Node Parameters
- Automatic discovery of node parameters
- Periodic parameter refresh (every 5 seconds)
- Asynchronous parameter fetching to avoid blocking

### System Telemetry
- CPU usage monitoring
- Memory (RAM) utilization tracking
- Disk space monitoring
- Enable/disable telemetry on demand

### Quality of Service (QoS) Support
- Full QoS profile inspection for publishers and subscribers
- Reliability, durability, history policy, depth, and liveliness information

### Robust Connection Management
- Automatic reconnection with exponential backoff
- Initial delay: 1 second, max delay: 10 seconds
- Secure WebSocket (WSS) connection
- Token-based authentication

## Architecture

```
WebBridge Node (osiris_agent)
│
├─ Main Thread (rclpy.spin)
│  ├─ ROS2 Node Callbacks
│  ├─ Graph Change Detector (timer: 100ms)
│  ├─ Parameter Refresh (timer: 5s)
│  └─ Telemetry Collector (timer: 1s)
│
└─ Daemon Thread (WebSocket Client)
   └─ Asyncio Event Loop
      ├─ Send Loop (queue consumer)
      ├─ Receive Loop (command handler)
      └─ Reconnection Logic (exponential backoff)
```

**Key Components:**
- **Graph Monitor**: Polls ROS2 graph for changes (nodes, topics, actions, services)
- **Subscription Manager**: Handles dynamic topic subscriptions with `threading.Lock()` for thread-safe access to the subscription dictionary
- **Telemetry Collector**: Gathers CPU, RAM, and disk metrics using `psutil`
- **WebSocket Client**: Maintains persistent connection with automatic reconnection
- **Message Queue**: `asyncio.Queue` created on the websocket event loop for serializing all outgoing messages

**Threading Model:**
- Main thread runs ROS2 executor (`rclpy.spin`) with timers and callbacks
- Daemon thread runs asyncio event loop with websocket client (send/receive coroutines)
- Shared data (`_topic_subs`) protected by `threading.Lock` to prevent race conditions
- Cross-thread communication via `asyncio.run_coroutine_threadsafe()` to schedule coroutines from ROS thread onto websocket loop

## Installation

```bash
# Clone the repository into your workspace
cd ~/ros2_ws/src
git clone https://github.com/nicolaselielll/osiris_agent.git

# Install the package
cd osiris_agent
pip3 install -e .
```

The `-e` flag installs in editable mode so changes to the code are reflected immediately.

## Configuration

Set your authentication token as an environment variable:

```bash
export OSIRIS_AUTH_TOKEN="your-robot-token-here"
```

**Tip**: Add to `~/.bashrc` to persist across sessions:
```bash
echo 'export OSIRIS_AUTH_TOKEN="your-token"' >> ~/.bashrc
```

### Optional: Customize Behavior

Edit constants in `osiris_agent/agent_node.py`:

- `MAX_SUBSCRIPTIONS = 100`: Maximum concurrent topic subscriptions
- `ALLOWED_TOPIC_PREFIXES = ['/']`: Restrict subscribable topics (e.g., `['/robot/', '/sensors/']`)
- `GRAPH_CHECK_INTERVAL = 0.1`: Graph polling interval (seconds)
- `PARAMETER_REFRESH_INTERVAL = 5.0`: Parameter refresh interval (seconds)
- `TELEMETRY_INTERVAL = 1.0`: Telemetry collection interval (seconds)

## Running

```bash
export OSIRIS_AUTH_TOKEN="your-token"
agent_node
```

You should see:
```
[INFO] [bridge_node]: Attempting to connect to gateway...
[INFO] [bridge_node]: Connected to gateway
[INFO] [bridge_node]: Sent initial state: 5 nodes, 12 topics, 0 actions, 8 services
```

## Updating

```bash
cd ~/ros2_ws/src/osiris_agent
git pull
# Changes apply immediately if installed with -e
```

## WebSocket Protocol

### Commands (Gateway → Agent)

**Subscribe to Topic**
```json
{"type": "subscribe", "topic": "/cmd_vel"}
```

**Unsubscribe**
```json
{"type": "unsubscribe", "topic": "/cmd_vel"}
```

**Control Telemetry**
```json
{"type": "start_telemetry"}
{"type": "stop_telemetry"}
```

### Events (Agent → Gateway)

**Initial State** (on connect)
```json
{
  "type": "initial_state",
  "timestamp": 1234567890.123,
  "data": {
    "nodes": {},
    "topics": {},
    "actions": {},
    "services": {},
    "telemetry": {}
  }
}
```

**Graph Changes**
```json
{"type": "node_event", "node": "/my_node", "event": "started", "timestamp": 123}
{"type": "topic_event", "topic": "/my_topic", "event": "created", "timestamp": 123}
{"type": "action_event", "action": "/my_action", "event": "created", "timestamp": 123}
{"type": "service_event", "service": "/my_service", "event": "created", "timestamp": 123}
```

**Topic Data**
```json
{
  "type": "topic_data",
  "topic": "/cmd_vel",
  "data": {"linear": {"x": 1.0}, "angular": {"z": 0.5}},
  "rate_hz": 10.0,
  "timestamp": 123
}
```

**Telemetry**
```json
{
  "type": "telemetry",
  "data": {
    "cpu": 45.2,
    "ram": {"percent": 62.5, "used_mb": 5000, "total_mb": 8000},
    "disk": {"percent": 75.0, "used_gb": 150, "total_gb": 200}
  },
  "timestamp": 123
}
```

**Bridge Subscriptions**
```json
{
  "type": "bridge_subscriptions",
  "subscriptions": ["/topic1", "/topic2"],
  "timestamp": 123
}
```

## Troubleshooting

**ModuleNotFoundError: websockets or psutil**
```bash
pip3 install -e .
```

**Connection drops**
- Check firewall allows outbound HTTPS to `osiris-gateway.fly.dev:443`
- View logs: `journalctl -u osiris-agent -f`

**High CPU usage**
- Increase `GRAPH_CHECK_INTERVAL` to `0.5` or `1.0`
- Reduce `PARAMETER_REFRESH_INTERVAL` to `10.0`
- Disable telemetry when not needed

**Topics not appearing**
- Verify topic exists: `ros2 topic list`
- Check logs for validation warnings
- Ensure under `MAX_SUBSCRIPTIONS` limit

## Performance Tuning

**Large graphs (100+ nodes/topics)**
```python
GRAPH_CHECK_INTERVAL = 0.5
PARAMETER_REFRESH_INTERVAL = 10.0
MAX_SUBSCRIPTIONS = 50
```

**Resource-constrained robots**
```python
GRAPH_CHECK_INTERVAL = 1.0
PARAMETER_REFRESH_INTERVAL = 15.0
TELEMETRY_INTERVAL = 2.0
```

**Restrict topic access**
```python
ALLOWED_TOPIC_PREFIXES = ['/robot/', '/sensors/']
```

## Security

- Auth token sent via WSS (encrypted in transit)
- **Never log `self.ws_url`** - it contains the authentication token in the query parameter
- Set `ALLOWED_TOPIC_PREFIXES` to restrict which topics can be remotely subscribed
- `MAX_SUBSCRIPTIONS` prevents resource exhaustion from excessive subscriptions
- Ensure gateway scrubs tokens from access logs

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Issues and PRs welcome at [github.com/nicolaselielll/osiris_agent](https://github.com/nicolaselielll/osiris_agent)

## Changelog

### v0.1.0
- WebSocket bridge with secure WSS connection
- Real-time graph monitoring
- Dynamic topic subscription
- System telemetry (CPU/RAM/disk)
- Auto-reconnect with exponential backoff
- Parameter discovery
- QoS profile inspection