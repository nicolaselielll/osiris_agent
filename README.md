# OSIRIS Agent

![PyPI](https://img.shields.io/pypi/v/osiris_agent.svg)
![Python](https://img.shields.io/pypi/pyversions/osiris_agent.svg)
![License](https://img.shields.io/pypi/l/osiris_agent.svg)
![CI](https://github.com/nicolaselielll/osiris_agent/actions/workflows/ci.yml/badge.svg)

A ROS2 Humble node that bridges your robot to the OSIRIS remote monitoring platform via WebSocket.

## Install

From PyPI:
```bash
python -m pip install --upgrade pip
python -m pip install osiris_agent
```

Editable / development install:
```bash
git clone https://github.com/nicolaselielll/osiris_agent.git
cd osiris_agent
python -m pip install -e .
```

## Quick Start

Set the auth token and run the agent:
```bash
export OSIRIS_AUTH_TOKEN="your-robot-token-here"
agent_node
```

Verify installation:
```bash
python -c "import importlib.metadata as m; print(m.version('osiris_agent'))"
```

## Usage & Configuration

- Environment: OSIRIS_AUTH_TOKEN — your robot token.
- Editable install reflects code changes immediately.
- Common constants are in `osiris_agent/agent_node.py`:
  - MAX_SUBSCRIPTIONS, ALLOWED_TOPIC_PREFIXES, GRAPH_CHECK_INTERVAL, PARAMETER_REFRESH_INTERVAL, TELEMETRY_INTERVAL

## Badge suggestions

- PyPI: https://img.shields.io/pypi/v/osiris_agent.svg
- Python versions: https://img.shields.io/pypi/pyversions/osiris_agent.svg
- License: https://img.shields.io/pypi/l/osiris_agent.svg
- GitHub Actions CI: https://github.com/<user>/osiris_agent/actions

## Contributing

Open issues and PRs at: https://github.com/nicolaselielll/osiris_agent

## License

Apache-2.0 — see the LICENSE file.

## Changelog

See release notes on GitHub Releases for v0.1.0 and future versions.