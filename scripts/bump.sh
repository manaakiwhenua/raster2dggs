#!/usr/bin/env bash
# Usage: scripts/bump.sh <patch|minor|major>
# Bumps the version in pyproject.toml, updates CITATION.cff, commits,
# pushes, and drafts a GitHub release with auto-generated notes.
set -euo pipefail

PART=${1:-}
if [[ -z "$PART" || ! "$PART" =~ ^(patch|minor|major)$ ]]; then
    echo "Usage: $0 <patch|minor|major>" >&2
    exit 1
fi

poetry version "$PART"
VERSION=$(poetry version -s)
TAG="v$VERSION"
TODAY=$(date +%F)
BRANCH=$(git rev-parse --abbrev-ref HEAD)

sed -i "s/^version: .*/version: \"$VERSION\"/" CITATION.cff
sed -i "s/^date-released: .*/date-released: \"$TODAY\"/" CITATION.cff

git add pyproject.toml poetry.lock CITATION.cff
git commit -m "version bump to $VERSION"
git push origin "$BRANCH"

gh release create "$TAG" --target "$BRANCH" --draft --generate-notes --title "$TAG"

echo ""
echo "Bumped to $VERSION, pushed, and drafted release $TAG."
echo "Review the auto-generated notes, then publish to trigger the PyPI upload:"
echo "  gh release edit $TAG --draft=false"
