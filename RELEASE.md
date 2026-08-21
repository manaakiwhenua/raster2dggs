# Release process

Releases are published to [PyPI](https://pypi.org/project/raster2dggs/) automatically via GitHub Actions when a GitHub Release is published. Authentication uses PyPI's [Trusted Publishers](https://docs.pypi.org/trusted-publishers/) mechanism (OIDC) — no API tokens are stored anywhere.

## Making a release

1. **Bump the version** — run the bump script, which updates `pyproject.toml`, `poetry.lock`, and `CITATION.cff` atomically and commits:

   ```bash
   scripts/bump.sh patch   # 0.9.0 → 0.9.1
   # or
   scripts/bump.sh minor   # 0.9.0 → 0.10.0
   # or
   scripts/bump.sh major   # 0.9.0 → 1.0.0
   ```

2. **Push:**

   ```bash
   git push
   ```

3. **Create a GitHub Release** — with the `gh` CLI:

   ```bash
   gh release create v0.9.1 --target master --title "v0.9.1" --generate-notes
   ```

   `--generate-notes` drafts the notes automatically from the PRs merged since the previous release; add `--notes "..."` to supplement them, or edit the release afterwards. The tag is created at the target commit when the release is published — do not push it manually beforehand.

   Or via the web UI:
   - Go to the repository on GitHub → Releases → **Draft a new release**
   - Create a new tag matching the version: `v0.9.1` (as with the CLI, the tag is created on publish)
   - Write release notes summarising changes (**Generate release notes** drafts them from merged PRs)
   - Click **Publish release**

4. The [`publish.yml`](.github/workflows/publish.yml) workflow triggers automatically, builds the distributions, and uploads them to PyPI. No further action is required.

## How it works

The workflow (`.github/workflows/publish.yml`) has three jobs:

- **`test`** — runs the full test suite and format checks (via `run-tests.yml`). A failure here stops the release before anything is built or published; fix the problem, then publish a fresh release (a published release whose workflow failed never reached PyPI, so its tag/release can be deleted and the version reused, or simply superseded).
- **`build`** — installs Poetry, runs `poetry build` to produce the source distribution and wheel, and uploads them as a workflow artifact.
- **`publish`** — downloads the artifact and uses [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish) to upload to PyPI. This job runs in the `release` GitHub environment and holds the `id-token: write` permission needed to obtain a short-lived OIDC token from GitHub. PyPI exchanges this token for upload credentials without any stored secret.

The two jobs are separated so that the elevated OIDC permission is scoped only to the publish step.

## One-time setup (already done)

These steps have been completed and do not need to be repeated unless the workflow file is renamed or the repository is transferred.

### PyPI — Trusted Publisher

In the project's PyPI settings ([pypi.org/manage/project/raster2dggs/settings/publishing/](https://pypi.org/manage/project/raster2dggs/settings/publishing/)), a GitHub Actions trusted publisher is configured with:

| Field | Value |
|---|---|
| Owner | `manaakiwhenua` |
| Repository | `raster2dggs` |
| Workflow filename | `publish.yml` |
| Environment name | `release` |

### GitHub — `release` environment

A [GitHub environment](https://github.com/manaakiwhenua/raster2dggs/settings/environments) named `release` is configured on the repository. The `publish` job runs inside this environment, which allows protection rules (e.g. required reviewers) to be added if a manual gate before publishing is ever desired.
