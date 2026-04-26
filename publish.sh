#!/bin/bash

# Usage: ./publish.sh 0.1.4

if [ -z "$1" ]; then
  echo "Usage: ./publish.sh <version>"
  exit 1
fi

V=$1

# Update version in setup.py
sed -i '' "s/version='.*'/version='$V'/" setup.py

# Commit and tag
git add .
git commit -m "chore: bump version to $V"
git tag -a v$V -m "v$V"
git push origin HEAD
git push origin "v$V"

# Build and upload
rm -rf dist build *.egg-info

# Build the C++ graph_watcher binary from source using the system ROS2 installation
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
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

python3.12 -m build
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=$(grep PYPI_API_TOKEN .env | cut -d= -f2)
python3.12 -m twine upload dist/*
unset TWINE_PASSWORD TWINE_USERNAME

echo "✅ Published version $V to PyPI"
