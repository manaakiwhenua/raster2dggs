#!/usr/bin/env bash
# Usage: scripts/bump.sh <patch|minor|major>
# Bumps the version in pyproject.toml, updates CITATION.cff, and commits.
set -euo pipefail

PART=${1:-}
if [[ -z "$PART" || ! "$PART" =~ ^(patch|minor|major)$ ]]; then
    echo "Usage: $0 <patch|minor|major>" >&2
    exit 1
fi

poetry version "$PART"
VERSION=$(poetry version -s)
TODAY=$(date +%F)

sed -i "s/^version: .*/version: \"$VERSION\"/" CITATION.cff
sed -i "s/^date-released: .*/date-released: \"$TODAY\"/" CITATION.cff

git add pyproject.toml poetry.lock CITATION.cff
git commit -m "version bump to $VERSION"

echo ""
echo "Bumped to $VERSION. Next: push, then publish a GitHub Release tagged v$VERSION."
