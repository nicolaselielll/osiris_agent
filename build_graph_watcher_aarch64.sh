#!/bin/bash
# Run this ON WaLI (aarch64, Ubuntu 24.04, ROS2 Jazzy) to rebuild the fixed
# graph_watcher binary in-place inside the installed osiris_agent package.
#
# Usage:
#   chmod +x build_graph_watcher_aarch64.sh
#   bash build_graph_watcher_aarch64.sh

set -e

SRC="$(cd "$(dirname "$0")" && pwd)/graph_watcher"
DEST="$(python3 -c "import importlib.resources; print(importlib.resources.files('osiris_agent'))")/bin/graph_watcher_aarch64"

echo "[build] Source: $SRC"
echo "[build] Destination: $DEST"

source /opt/ros/jazzy/setup.bash

BUILD_DIR="$(mktemp -d)"
trap "rm -rf $BUILD_DIR" EXIT

cmake "$SRC" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release
cmake --build "$BUILD_DIR" --parallel $(nproc)

cp "$BUILD_DIR/graph_watcher" "$DEST"
chmod +x "$DEST"

echo "[build] Done. Binary installed to $DEST"
