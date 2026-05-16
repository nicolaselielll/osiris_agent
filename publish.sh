#!/bin/bash
set -e

# Usage: ./publish.sh 0.1.4

if [ -z "$1" ]; then
  echo "Usage: ./publish.sh <version>"
  exit 1
fi

V=$1

cd "$(dirname "$0")"

# Update version in setup.py and __init__.py
sed -i "s/version='.*'/version='$V'/" setup.py
sed -i "s/__version__ = '.*'/__version__ = '$V'/" osiris_agent/__init__.py

# Commit and tag
git add .
git commit -m "chore: bump version to $V"
git tag -a v$V -m "v$V"
git push origin HEAD
git push origin "v$V"

# Build and upload
rm -rf dist build *.egg-info

# Note: graph_watcher binaries (graph_watcher_x86_64, graph_watcher_aarch64)
# are built by GitHub Actions CI and committed back to the repo automatically.
# Make sure to pull the latest dev/main before running this script so the
# up-to-date binaries are included in the PyPI wheel.
echo "ℹ️  Using pre-built graph_watcher binaries from repo (built by CI)"

python3 -m build
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=$(grep PYPI_API_TOKEN .env | cut -d= -f2)
python3 -m twine upload dist/*
unset TWINE_PASSWORD TWINE_USERNAME

echo "✅ Published version $V to PyPI"
