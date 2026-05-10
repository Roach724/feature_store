# Feature Store — Documentation & CI/CD Design

Date: 2026-05-10
Status: approved

## Overview

Create comprehensive project documentation (README, API reference, changelog) and CI/CD pipeline specification with automated build (wheel + zip).

---

## 1. Documentation Structure

```
feature_store/
├── README.md                          # Project intro + quick start + nav
├── CHANGELOG.md                       # Version history (Keep a Changelog)
├── docs/
│   ├── API.md                         # Module overview + full client API reference
│   └── CI_CD.md                       # CI/CD pipeline specification
├── scripts/
│   └── build.py                       # Build script (wheel + zip)
└── .github/
    └── workflows/
        └── ci_cd.yml                  # GitHub Actions workflow
```

### README.md

**Sections:**
1. **Project intro** — one-line description: "A PySpark-native Feature Store client with YAML-based registry, supporting feature views, model feature sets, labels, datasets, and training sets."
2. **Core capabilities** — bullet points: YAML registry, 5 entity types, pluggable storage (GCS/Local), Spark lineage checkpoint, auto-register on write, dependency DAG, lifecycle management
3. **Installation** — pip install from source / wheel / zip for spark-submit
4. **Quick start** — minimal example: init client, register feature view from DataFrame, write, read
5. **Documentation navigation** — links to `docs/API.md`, `docs/CI_CD.md`, `CHANGELOG.md`

### CHANGELOG.md

Follow [Keep a Changelog](https://keepachangelog.com/) format.

```markdown
# Changelog

## [2.0.0] - 2026-05-10

### Added
- Unified YAML format with `kind` field across all entity types
- Integrated registration API (`register`, `register_feature_view`, etc.)
- Pluggable storage backend (`LocalBackend`, `GCSBackend`)
- `CheckpointContext` and `checkpoint_context()` for shared checkpoint lifecycle
- Auto-register on first write (`auto_register=True` by default)
- `build_dependency_graph()` for DAG traversal
- `migrate_registry()` for old-to-new format migration
- `sync_lifecycle()` for GCS lifecycle rules
- `Lookback` dataclass for structured time-shift control
- CI/CD pipeline (GitHub Actions)
- Build script for wheel + zip distribution

### Changed
- Flattened project structure: `core/` + `scripts/` → `src/feature_store/`
- Model features: URN list `"view@v1:feat"` → structured schema with `feature_view` / `feature_view_version`
- `primary_keys` / `partition_columns` extracted from schema to top-level YAML fields
- `entity_type` string → `EntityKind` enum
- `print()` → Python `logging`
- `export_training_dataset`: scattered lookback params → `Lookback` dataclass

### Removed
- `core/` and `scripts/` old project structure
- `entity` field in YAML (domain entity concept)
- `sync_project_resources.sh` deployment script
```

### API.md

**Sections:**

#### 1. Architecture Overview
ASCII diagram of 6 modules and their relationships.

#### 2. Module: types
| Type | Description |
|------|-------------|
| `EntityKind` | enum: FEATURE_VIEW, MODEL, LABEL, DATASET, TRAINING_SET |
| `StorageFormat` | enum: PARQUET |
| `UpdateFrequency` | enum: DAILY, WEEKLY, MONTHLY |

#### 3. Module: schema
All config dataclasses with field tables.
- `ColumnSpec`, `KeySpec`, `Dependency`
- `PipelineSpec`, `RetentionSpec`, `StorageSpec`, `Lookback`
- `FeatureViewConfig`, `ModelConfig`, `LabelConfig`, `DatasetConfig`, `TrainingSetConfig`

#### 4. Module: storage
- `StorageBackend` (ABC)
- `LocalBackend` — local filesystem
- `GCSBackend` — Google Cloud Storage via fsspec
- `get_backend(path)` — factory

#### 5. Module: registry
Low-level registry functions (normally not called directly):
- `config_to_yaml`, `yaml_to_config`, `load_yaml`, `write_yaml`
- `validate_dataframe`, `build_config_from_df`, `build_model_config`
- `list_entities`, `sync_lifecycle`
- Exception hierarchy

#### 6. Module: client (重点)

Each method documented with:
- Function description
- Parameter table (name, type, default, description)
- Return type and description
- Code example

**Coverage:**
- `__init__` — constructor
- `register` / `register_feature_view` / `register_model` / `register_label` / `register_dataset` / `register_training_set` — registration
- `write_entity` — schema-validated write with auto-register
- `get_entity` — read with column projection + partition filter + type coercion
- `write_dataset` / `get_dataset` — dataset convenience
- `write_training_set` / `get_training_set` — training set with split
- `get_model_features` — multi-view assembly with checkpoint
- `export_training_dataset` — feature+label PIT join with lookback
- `checkpoint_context` — shared checkpoint lifecycle
- `list_entities` / `get_entity_info` — registry inspection
- `sync_lifecycle` — GCS TTL rules
- `build_dependency_graph` — DAG computation
- `migrate_registry` — legacy format migration

### CI_CD.md

**Sections:**
1. Pipeline overview (trigger conditions)
2. Job descriptions (lint, typecheck, test, build, release)
3. Required secrets and environment variables
4. Build script reference
5. Deployment: wheel to GitHub Release, zip to GCS
6. Integration test environment setup

---

## 2. Build Script

### `scripts/build.py`

```python
"""Build feature_store as wheel and zip for cluster deployment.

Usage:
    python scripts/build.py

Output:
    dist/feature_store-2.0.0-py3-none-any.whl
    dist/feature_store.zip
"""

import os
import shutil
import subprocess
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
DIST = os.path.join(ROOT, "dist")


def clean_dist():
    if os.path.exists(DIST):
        shutil.rmtree(DIST)
    os.makedirs(DIST)


def build_wheel():
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "build"],
        check=True, capture_output=True,
    )
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", DIST, ROOT],
        check=True,
    )


def build_zip():
    zip_path = os.path.join(DIST, "feature_store.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SRC):
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for f in files:
                if f.endswith(".pyc"):
                    continue
                full = os.path.join(root, f)
                arcname = os.path.relpath(full, SRC)
                zf.write(full, arcname)
    return zip_path


if __name__ == "__main__":
    clean_dist()
    build_wheel()
    zip_path = build_zip()
    for f in os.listdir(DIST):
        size = os.path.getsize(os.path.join(DIST, f))
        print(f"  dist/{f} ({size:,} bytes)")
```

---

## 3. CI/CD Pipeline

### Trigger Conditions

| Event | Jobs |
|-------|------|
| Push to `main`, `develop` | lint + typecheck + test |
| PR to `main` | lint + typecheck + test + build |
| Tag `v*` (e.g. `v2.0.0`) | lint + typecheck + test + build + release |

### GitHub Actions Workflow

**File:** `.github/workflows/ci_cd.yml`

```yaml
name: CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]
  workflow_dispatch:

jobs:
  # ── Lint ──────────────────────────────────────────────
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install ruff
      - run: ruff check src/ tests/

  # ── Type Check ────────────────────────────────────────
  typecheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install pyright
      - run: pyright src/

  # ── Test ──────────────────────────────────────────────
  test:
    needs: [lint, typecheck]
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "${{ matrix.python-version }}" }
      - uses: actions/setup-java@v4
        with: { distribution: "temurin", java-version: "8" }
      - run: pip install -e ".[dev]"
      - run: PYTHONPATH=src python -m pytest tests/ -v --tb=short

  # ── Build ─────────────────────────────────────────────
  build:
    needs: [test]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install build
      - run: python scripts/build.py
      - uses: actions/upload-artifact@v4
        with:
          name: dist
          path: dist/

  # ── Release (tag only) ────────────────────────────────
  release:
    if: startsWith(github.ref, 'refs/tags/v')
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/download-artifact@v4
        with: { name: dist, path: dist/ }
      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: dist/*
          generate_release_notes: true
      - name: Upload wheel to GCS
        run: |
          WHEEL=$(ls dist/*.whl | head -1)
          ZIP=$(ls dist/*.zip | head -1)
          gcloud storage cp "$WHEEL" "$ZIP" "${{ secrets.GCS_MODULE_PATH }}/"
```

### Required Secrets

| Secret | Purpose |
|--------|---------|
| `GCS_MODULE_PATH` | GCS path for wheel/zip artifacts (e.g. `gs://bucket/modules/`) |
| `GCP_SA_KEY` | Service account key for GCS upload (if not using Workload Identity) |

### Environment

- **Test:** Ubuntu 22.04, Python 3.10/3.11, Java 8 (Temurin), Spark via pip pyspark
- **Release:** Same as test, plus authenticated GCS access
- **Integration tests (future):** Self-hosted runner on GCP VM with Spark cluster and GCS bucket access, triggered by manual workflow dispatch or tag
