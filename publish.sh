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
python3.12 -m build
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=$(grep PYPI_API_TOKEN .env | cut -d= -f2)
python3.12 -m twine upload dist/*
unset TWINE_PASSWORD TWINE_USERNAME

echo "✅ Published version $V to PyPI"
