#!/bin/bash

# Usage: ./publish.sh 0.1.4

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [ -z "$1" ]; then
  echo "Usage: ./publish.sh <version>"
  exit 1
fi

V=$1

# Update version in osiris_agent/__init__.py (single source of truth)
"$PYTHON_BIN" - "$V" "$SCRIPT_DIR/osiris_agent/__init__.py" <<'PY'
import pathlib
import re
import sys

version = sys.argv[1]
path = pathlib.Path(sys.argv[2])
text = path.read_text(encoding="utf-8")
updated, count = re.subn(
  r"__version__\s*=\s*['\"][^'\"]+['\"]",
  f"__version__ = '{version}'",
  text,
  count=1,
)
if count != 1:
  raise SystemExit(f"Failed to update __version__ in {path}")
path.write_text(updated, encoding="utf-8")
PY

# Commit and tag
git add .
git commit -m "chore: bump version to $V"
git tag -a v$V -m "v$V"
git push origin HEAD
git push origin "v$V"

# Build and upload
rm -rf dist build *.egg-info

# Build the C++ graph_watcher binary from source using the system ROS2 installation
ROS_SETUP="/opt/ros/humble/setup.bash"

if [ ! -f "$ROS_SETUP" ]; then
  echo "⚠️  ROS2 Humble not found at $ROS_SETUP — skipping graph_watcher build"
else
  echo "Building graph_watcher..."
  BUILD_DIR="$(mktemp -d)"
  source "$ROS_SETUP"
  cmake -S "$SCRIPT_DIR/graph_watcher" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release > /dev/null
  cmake --build "$BUILD_DIR" --parallel > /dev/null
  cp "$BUILD_DIR/graph_watcher" "$SCRIPT_DIR/osiris_agent/bin/graph_watcher"
  chmod +x "$SCRIPT_DIR/osiris_agent/bin/graph_watcher"
  rm -rf "$BUILD_DIR"
  echo "✅ Built and bundled graph_watcher binary"
fi

if [ ! -f "$SCRIPT_DIR/osiris_agent/bin/graph_watcher" ]; then
  echo "❌ graph_watcher binary missing — aborting. Build it on a Linux+ROS2 machine and copy to osiris_agent/bin/"
  exit 1
fi

# Verify the binary is a Linux ELF, not a macOS Mach-O.
# On Mac the 'file' command is available; use it to catch the wrong binary.
_BIN="$SCRIPT_DIR/osiris_agent/bin/graph_watcher"
if command -v file >/dev/null 2>&1; then
  _FILE_OUTPUT="$(file "$_BIN")"
  if echo "$_FILE_OUTPUT" | grep -q "ELF"; then
    echo "✅ Binary is Linux ELF: $_FILE_OUTPUT"
  else
    echo "❌ Binary does not appear to be a Linux ELF: $_FILE_OUTPUT"
    echo "   Copy the Linux-built binary from your ROS2 machine before publishing."
    exit 1
  fi
fi

"$PYTHON_BIN" -m build
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=$(grep PYPI_API_TOKEN .env | cut -d= -f2)
"$PYTHON_BIN" -m twine upload dist/*
unset TWINE_PASSWORD TWINE_USERNAME

echo "✅ Published version $V to PyPI"
