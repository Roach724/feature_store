# CI/CD Pipeline

## Overview

GitHub Actions workflow providing lint, type check, unit test, build, and
release automation for the Feature Store client.

## Trigger Conditions

| Event | Jobs |
|-------|------|
| Push to `main`, `develop` | lint, typecheck, test |
| PR to `main` | lint, typecheck, test, build |
| Tag `v*` (e.g. `v2.0.0`) | lint, typecheck, test, build, release |

Manual dispatch is also supported via `workflow_dispatch`.

## Pipeline Stages

```
Lint → Typecheck → Test → Build → Release (tag only)
ruff    pyright    pytest  build.py  GitHub Release + GCS
```

### Lint

**Tool:** [ruff](https://github.com/astral-sh/ruff)
**Command:** `ruff check src/ tests/`

Checks code style and import ordering across all source and test files.

### Type Check

**Tool:** [pyright](https://github.com/microsoft/pyright)
**Command:** `pyright src/`

Static type analysis on source code. Tests are excluded from type checking.

### Test

**Tool:** [pytest](https://docs.pytest.org/)
**Command:** `PYTHONPATH=src python -m pytest tests/ -v --tb=short`

Runs all 88 tests across two Python versions (3.10, 3.11) with local Spark
(`master=local[2]`). Requires Java 8+ (Temurin).

### Build

**Tool:** `scripts/build.py`
**Command:** `python scripts/build.py`

Produces two artifacts in `dist/`:

| Artifact | Usage |
|----------|-------|
| `feature_store-2.0.0-py3-none-any.whl` | `pip install` on cluster nodes |
| `feature_store.zip` | `spark-submit --py-files` |

### Release

Triggered by tags matching `v*`. Stages:

1. Downloads build artifacts from the `build` job
2. Authenticates to Google Cloud via `google-github-actions/auth@v2`
3. Creates a GitHub Release with auto-generated release notes from commits
4. Uploads `dist/*` assets to the release
5. Copies wheel and zip to GCS module path (`$GCS_MODULE_PATH/`)

## Required Secrets

| Secret | Purpose |
|--------|---------|
| `GCS_MODULE_PATH` | GCS path for artifacts (e.g. `gs://bucket/modules/`) |
| `GCP_SA_KEY` | Service account key for GCS access (if not using Workload Identity) |

## Build Script

`scripts/build.py` produces both distribution formats:

```bash
$ python scripts/build.py
  dist/feature_store-2.0.0-py3-none-any.whl (12,345 bytes)
  dist/feature_store.zip (9,876 bytes)
```

### Wheel

Uses `python -m build --wheel` with the project's `pyproject.toml`. Contains
the `src/feature_store/` package and metadata.

### Zip

Packs `src/feature_store/` into a flat zip (excluding `__pycache__` and `.pyc`
files). Suitable for `spark-submit --py-files` which adds the zip contents to
the Python path on all executors.

## Workflow File

Located at `.github/workflows/ci_cd.yml`.

## Environment Requirements

| Component | Version |
|-----------|---------|
| Python | 3.10, 3.11 |
| Java | 8 (Temurin) |
| PySpark | >=3.3 |
| OS | ubuntu-latest |

## Integration Tests (Future)

For tests requiring a real Spark cluster and GCS bucket, a self-hosted runner
on GCP can be used. Mark these tests with `@pytest.mark.integration` and
trigger them via manual workflow dispatch or tag.

```yaml
# Future addition to ci_cd.yml
integration-test:
  if: startsWith(github.ref, 'refs/tags/v') || github.event_name == 'workflow_dispatch'
  runs-on: self-hosted
  steps:
    - run: PYTHONPATH=src python -m pytest tests/ -v -m integration
```
