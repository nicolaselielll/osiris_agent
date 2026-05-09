#!/usr/bin/env bash
# build_aarch64_jazzy.sh
#
# Builds graph_watcher_aarch64 using a ROS2 Jazzy arm64 Docker container
# and copies the result into osiris_agent/bin/.
#
# Prerequisites:
#   docker with buildx + qemu binfmt support:
#     docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
#
# Run from: third_party/osiris_agent/
# Usage:    bash build_aarch64_jazzy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC="$SCRIPT_DIR/graph_watcher"
OUT="$SCRIPT_DIR/osiris_agent/bin"

echo "[build] Registering QEMU binfmt handlers (requires docker + privileged)..."
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes

echo "[build] Building graph_watcher for linux/arm64 against ROS2 Jazzy..."
docker run --rm \
  --platform linux/arm64 \
  -v "$SRC":/ws/src/graph_watcher:ro \
  -v "$OUT":/out \
  ros:jazzy-ros-base \
  bash -c '
    set -e
    source /opt/ros/jazzy/setup.bash
    cd /ws
    colcon build --packages-select graph_watcher \
      --cmake-args -DCMAKE_BUILD_TYPE=Release 2>&1
    cp install/graph_watcher/lib/graph_watcher/graph_watcher /out/graph_watcher_aarch64
    echo "[build] Success: $(file /out/graph_watcher_aarch64)"
  '

echo "[build] Done: $OUT/graph_watcher_aarch64"
