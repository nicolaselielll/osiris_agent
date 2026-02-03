# OSIRIS Agent

![PyPI](https://img.shields.io/pypi/v/osiris_agent.svg)
![Python](https://img.shields.io/pypi/pyversions/osiris_agent.svg)
![License](https://img.shields.io/pypi/l/osiris_agent.svg)
![CI](https://github.com/nicolaselielll/osiris_agent/actions/workflows/ci.yml/badge.svg)

A ROS2 Humble node that bridges your robot to the OSIRIS remote monitoring platform via WebSocket.

## Install

Install the OSIRIS agent on your robot:
```bash
pip3 install osiris-agent
```

## Quick Start

### 1. Add token to your environment

```bash
export OSIRIS_AUTH_TOKEN=your-token-here
```

### 2. (Optional) Enable behaviour tree collector

If you're using BT.CPP with Groot2 monitoring:

```bash
export OSIRIS_BT_COLLECTOR_ENABLED=true
```

Default host is `127.0.0.1`. Default port for server is `1667` and for publisher `1668`. To change these:

```bash
export OSIRIS_BT_HOST=127.0.0.1
export OSIRIS_BT_SERVER_PORT=1667
export OSIRIS_BT_PUBLISHER_PORT=1668
```

### 3. Start agent

```bash
osiris_node
```

## Usage & Configuration

### Required Environment Variables

- `OSIRIS_AUTH_TOKEN` — Your robot authentication token from the OSIRIS platform

### Optional Environment Variables

**Behaviour Tree Monitoring (BT.CPP + Groot2):**
- `OSIRIS_BT_COLLECTOR_ENABLED` — Set to `true` to enable BT.CPP tree monitoring (default: `false`)
- `OSIRIS_BT_HOST` — Groot2 ZMQ host address (default: `127.0.0.1`)
- `OSIRIS_BT_SERVER_PORT` — Groot2 REQ/REP server port (default: `1667`)
- `OSIRIS_BT_PUBLISHER_PORT` — Groot2 PUB/SUB publisher port (default: `1668`)

## BT.CPP Integration

The agent automatically collects behaviour tree events from BT.CPP when `OSIRIS_BT_COLLECTOR_ENABLED=true`. Your BT.CPP application must:

1. Enable Groot2 monitoring in your code
2. Use the standard Groot2 ZMQ ports (1667/1668) or configure custom ports via environment variables

The collector uses BT.CPP's standardized Groot2 protocol, so it works with any behaviour tree without custom configuration.

## Contributing

Open issues and PRs at: https://github.com/nicolaselielll/osiris_agent

## License

Apache-2.0 — see the LICENSE file.

## Changelog

See release notes on GitHub Releases for v0.1.0 and future versions.