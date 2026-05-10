# Documentation & CI/CD — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create complete project documentation (README, CHANGELOG, API reference, CI/CD spec), build script (wheel + zip), and GitHub Actions CI/CD pipeline.

**Architecture:** 4 documentation files at root + `docs/`, 1 build script at `scripts/`, 1 CI workflow at `.github/workflows/`. All tasks are file creation, mutually independent except for cross-links.

**Tech Stack:** Markdown, Python (build script), YAML (GitHub Actions)

---

## File Structure

```
feature_store/
├── README.md                     # Create
├── CHANGELOG.md                  # Create
├── docs/
│   ├── API.md                    # Create
│   └── CI_CD.md                  # Create
├── scripts/
│   └── build.py                  # Create
└── .github/
    └── workflows/
        └── ci_cd.yml             # Create
```

---

### Task 1: README.md

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write README.md**

```markdown
# Feature Store Client

A PySpark-native Feature Store client with YAML-based registry, supporting
feature views, model feature sets, labels, datasets, and training sets.

## Core Capabilities

- **YAML Registry** — unified `kind`-based YAML format for all entity types
- **Five Entity Types** — feature_view, model, label, dataset, training_set
- **Pluggable Storage** — GCS and local backends, extensible to S3
- **Spark Lineage Checkpoint** — context-managed checkpoint with UUID isolation
  for parallel writes
- **Auto-register on Write** — first write automatically registers entity
  metadata
- **Dependency DAG** — recursive BFS traversal over entity dependencies
- **Lifecycle Management** — sync TTL/cold-tier rules from registry to GCS
  buckets
- **Migration Tool** — convert legacy YAML format to current schema

## Installation

```bash
# From source
pip install -e ".[dev]"

# From wheel (for Spark cluster)
pip install dist/feature_store-2.0.0-py3-none-any.whl

# As --py-files (for spark-submit)
spark-submit --py-files dist/feature_store.zip ...
```

## Quick Start

```python
from pyspark.sql import SparkSession
from feature_store import FeatureStoreClient, EntityKind

spark = SparkSession.builder.appName("demo").getOrCreate()
client = FeatureStoreClient(spark, registry_dir="gs://bucket/registry")

# Register and write a feature view in one step
df = spark.createDataFrame(
    [(1, "2024-01-01", 25, 0.8)],
    ["user_id", "dt", "age", "score"]
)
client.write_entity(
    df, EntityKind.FEATURE_VIEW, "user_features", "v1",
    primary_keys=["user_id"],
    partition_columns=["dt"],
    storage_base_path="gs://bucket/features/user_features/v1",
)

# Read features
features = client.get_entity(
    EntityKind.FEATURE_VIEW, "user_features", "v1",
    columns=["age", "score"],
    start_date="2024-01-01",
)
features.show()
```

## Documentation

| Document | Description |
|----------|-------------|
| [API Reference](docs/API.md) | Module overview and full client API reference |
| [CI/CD](docs/CI_CD.md) | Continuous integration and deployment pipeline |
| [Changelog](CHANGELOG.md) | Version history and release notes |
| [Design Specs](docs/superpowers/specs/) | Design documents for each feature |

## License

Internal use.
```

- [ ] **Step 2: Commit**

```bash
cd D:/feature_store && git add README.md && git commit -m "docs: add README with project intro, quick start, and nav"
```

---

### Task 2: CHANGELOG.md

**Files:**
- Create: `CHANGELOG.md`

- [ ] **Step 1: Write CHANGELOG.md**

```markdown
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-05-10

### Added
- Unified YAML format with `kind` field across all entity types
  (`feature_view`, `model`, `label`, `dataset`, `training_set`)
- Integrated registration API: `register()`, `register_feature_view()`,
  `register_model()`, `register_label()`, `register_dataset()`,
  `register_training_set()`
- Pluggable storage backend abstraction (`LocalBackend`, `GCSBackend`,
  `get_backend()`)
- `CheckpointContext` and `checkpoint_context()` context manager for shared
  checkpoint lifecycle across `get_model_features` and `export_training_dataset`
- Auto-register on first write (`auto_register=True` by default on
  `write_entity`, `write_dataset`, `write_training_set`)
- Metadata inference helpers for primary keys, partition columns, and storage
  paths
- `build_dependency_graph()` for recursive BFS dependency DAG traversal
- `migrate_registry()` for batch old-to-new YAML format migration
- `sync_lifecycle()` for extracting and applying GCS bucket lifecycle rules
- `Lookback` dataclass for structured time-shift control in
  `export_training_dataset`
- Structured `export_training_dataset` lookback using `pandas.DateOffset`
- CI/CD pipeline specification with lint, typecheck, test, build, and release
- Build script (`scripts/build.py`) producing wheel and zip artifacts
- 88 unit and integration tests across all modules
- Type-safe config dataclasses with `__post_init__` validation
- Exception hierarchy (`FeatureStoreError`, `EntityNotFoundError`, etc.)
- Python `logging` throughout (replacing all `print()` calls)

### Changed
- **Project structure:** flattened `core/` + `scripts/` → `src/feature_store/`
- **Model features:** URN list `"view@v1:feat"` → structured `schema` with
  `feature_view` / `feature_view_version` per column
- **YAML format:** `primary_keys` and `partition_columns` extracted from inline
  schema to top-level fields with `name` / `type` / `format`
- **Entity identification:** `entity_type: str` → `EntityKind` enum
- **Model YAML:** now requires `version` field (was missing)
- **Model assembly:** `get_batch_model_features` → `get_model_features` with
  internally managed checkpoint lifecycle
- **Registration:** standalone script functions → integrated `client.register()`
- `FeatureStoreClient.__init__`: `feature_store: str` → `registry_dir: str`

### Removed
- `core/` directory (old `client.py`, `yaml_builder.py`, `tools/`)
- `scripts/` directory (old `sync_lifecycle.py`)
- `sync_project_resources.sh` deployment script
- `entity` field in YAML (ambiguous domain entity concept)
- `print()`-based logging throughout
```

- [ ] **Step 2: Commit**

```bash
cd D:/feature_store && git add CHANGELOG.md && git commit -m "docs: add CHANGELOG with v2.0.0 release notes"
```

---

### Task 3: docs/API.md

**Files:**
- Create: `docs/API.md`

- [ ] **Step 1: Write docs/API.md**

Paste the complete content below:

````markdown
# API Reference

## Architecture Overview

```
feature_store/
├── types.py       → EntityKind, StorageFormat, UpdateFrequency enums
├── schema.py      → ColumnSpec, KeySpec, config dataclasses, Lookback
├── storage.py     → StorageBackend (ABC), LocalBackend, GCSBackend
├── registry.py    → YAML serialization, validation, lifecycle, migration
└── client.py      → FeatureStoreClient (user-facing API)
```

The client is the single entry point for users. `schema` provides typed config
objects for programmatic registration. `storage` and `registry` are internal
layers that the client delegates to.

---

## Module: types

Enums used across all modules.

### EntityKind

```python
class EntityKind(enum.Enum):
    FEATURE_VIEW = "feature_view"
    MODEL        = "model"
    LABEL        = "label"
    DATASET      = "dataset"
    TRAINING_SET = "training_set"
```

### StorageFormat

```python
class StorageFormat(enum.Enum):
    PARQUET = "parquet"
```

### UpdateFrequency

```python
class UpdateFrequency(enum.Enum):
    DAILY   = "daily"
    WEEKLY  = "weekly"
    MONTHLY = "monthly"
```

---

## Module: schema

Typed dataclasses for config-first registration. All have `__post_init__`
validation.

### Core specifications

| Class | Fields | Description |
|-------|--------|-------------|
| `ColumnSpec` | `name`, `type`, `is_label`, `feature_view`, `feature_view_version` | A single schema column. Model columns require `feature_view` + `feature_view_version` |
| `KeySpec` | `name`, `type`, `format` | A primary key or partition column. `format` is optional (e.g. `"yyyy-MM"`) |
| `Dependency` | `name`, `version` | Reference to an upstream entity |
| `PipelineSpec` | `update_frequency`, `source_job`, `alert_threshold_hours` | Pipeline metadata |
| `RetentionSpec` | `ttl_days`, `cold_tier_days` | Data retention policy |
| `StorageSpec` | `base_path`, `format` | Storage location |
| `Lookback` | `freq`, `periods` | Time shift for label join. `freq` is `"day"` or `"month"` |

### Entity Config classes

| Class | Required fields | Optional fields | `kind` |
|-------|----------------|-----------------|--------|
| `FeatureViewConfig` | `name`, `version`, `primary_keys`, `partition_columns`, `storage` | `owner`, `description`, `dependency`, `pipeline`, `retention`, `schema` | `feature_view` |
| `ModelConfig` | `name`, `version`, `primary_keys`, `partition_columns` | `owner`, `description`, `dependency`, `schema` | `model` |
| `LabelConfig` | `name`, `version`, `primary_keys`, `partition_columns`, `storage` | `owner`, `description`, `dependency`, `retention`, `schema` | `label` |
| `DatasetConfig` | `name`, `version`, `primary_keys`, `partition_columns`, `storage` | `owner`, `description`, `dependency` | `dataset` |
| `TrainingSetConfig` | same as `DatasetConfig` | — | `training_set` |

**Usage:**
```python
from feature_store import FeatureViewConfig, KeySpec, ColumnSpec, StorageSpec

config = FeatureViewConfig(
    name="user_features", version="v1",
    primary_keys=[KeySpec(name="user_id", type="string")],
    partition_columns=[KeySpec(name="dt", type="string", format="yyyy-MM-dd")],
    storage=StorageSpec(base_path="gs://bucket/features/user/v1"),
    schema=[
        ColumnSpec(name="age", type="integer"),
        ColumnSpec(name="score", type="double"),
    ],
)
client.register(config)
```

---

## Module: storage

Storage backend abstraction.

| Class | Description |
|-------|-------------|
| `StorageBackend` (ABC) | Abstract interface: `read_parquet`, `write_parquet`, `open`, `exists`, `glob`, `cp`, `rm` |
| `LocalBackend` | Local filesystem implementation |
| `GCSBackend` | Google Cloud Storage via fsspec. Adds `partitionOverwriteMode=dynamic` to writes |
| `get_backend(path)` | Factory: `gs://` → GCSBackend, `/local/*` → LocalBackend |

---

## Module: registry

Low-level registry operations. Normally called through the client, not directly.

### Serialization

| Function | Description |
|----------|-------------|
| `config_to_yaml(config)` | Serialize any config to YAML string |
| `yaml_to_config(data)` | Parse YAML dict into typed config |
| `load_yaml(backend, path)` | Load YAML from storage |
| `write_yaml(backend, path, config)` | Write config as YAML to storage |

### Validation & Building

| Function | Description |
|----------|-------------|
| `validate_dataframe(df, config, allow_extra_columns)` | Check DataFrame columns against config |
| `build_config_from_df(df, kind, name, version, ...)` | Infer config from DataFrame schema |
| `build_model_config(features, name, version, ...)` | Build ModelConfig from feature specs |

### Management

| Function | Description |
|----------|-------------|
| `list_entities(backend, registry_dir, kind)` | Scan registry for entities |
| `sync_lifecycle(backend, registry_dir)` | Apply GCS lifecycle rules from YAML |

### Exceptions

`FeatureStoreError` → `EntityNotFoundError`, `ColumnNotFoundError`,
`SchemaValidationError` → `MissingColumnsError`, `ExtraColumnsError`,
`RegistryFormatError`, `StorageError`

---

## Module: client — FeatureStoreClient

The primary user-facing API. All interactions go through `FeatureStoreClient`.

### Constructor

```python
FeatureStoreClient(spark: SparkSession, registry_dir: str)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active Spark session |
| `registry_dir` | `str` | Base path for YAML registry. `gs://` for GCS, `/local/path` for local. Auto-selects backend |

**Example:**
```python
from pyspark.sql import SparkSession
from feature_store import FeatureStoreClient

spark = SparkSession.builder.appName("fs").getOrCreate()
client = FeatureStoreClient(spark, "gs://my-bucket/feature_store/registry")
```

---

### Registration

#### `register(source, kind, name, version, primary_keys, partition_columns, **kwargs) -> str`

Unified registration entry point. Accepts a config dataclass or a Spark DataFrame.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `Config \| DataFrame` | — | Config object or DataFrame |
| `kind` | `EntityKind` | required for DataFrame | Entity type |
| `name` | `str` | required for DataFrame | Entity name |
| `version` | `str` | `"v1"` | Entity version |
| `primary_keys` | `List[str]` | `[]` | Primary key column names |
| `partition_columns` | `List[str]` | `[]` | Partition column names |
| `storage_base_path` | `str` | `None` | Storage path (required for entities with storage) |

Returns the YAML registry file path.

**Example — from Config:**
```python
from feature_store import ModelConfig, KeySpec, ColumnSpec

config = ModelConfig(
    name="churn_model", version="v1",
    primary_keys=[KeySpec(name="user_id", type="string")],
    partition_columns=[KeySpec(name="dt", type="string")],
    schema=[
        ColumnSpec(name="age", type="integer", feature_view="user_fv", feature_view_version="v1"),
    ],
)
client.register(config)
```

**Example — from DataFrame:**
```python
df = spark.createDataFrame([(1, "2024-01-01", 25)], ["user_id", "dt", "age"])
client.register(df, kind=EntityKind.FEATURE_VIEW, name="user_fv", version="v1",
                primary_keys=["user_id"], partition_columns=["dt"],
                storage_base_path="gs://bucket/features/user_fv/v1")
```

#### Convenience registration methods

| Method | Equivalent `kind` |
|--------|-------------------|
| `register_feature_view(source, ...)` | `EntityKind.FEATURE_VIEW` |
| `register_model(source, ...)` | `EntityKind.MODEL` |
| `register_label(source, ...)` | `EntityKind.LABEL` |
| `register_dataset(source, ...)` | `EntityKind.DATASET` |
| `register_training_set(source, ...)` | `EntityKind.TRAINING_SET` |

---

### Write

#### `write_entity(df, kind, name, version, partition_num, allow_extra_columns, auto_register, primary_keys, partition_columns, storage_base_path, **kwargs) -> None`

Validate and persist a DataFrame. Supports auto-registration on first write.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | — | Spark DataFrame to write |
| `kind` | `EntityKind` | — | Entity type |
| `name` | `str` | — | Entity name |
| `version` | `str` | — | Entity version |
| `partition_num` | `int` | `24` | Number of output partitions |
| `allow_extra_columns` | `bool` | `False` | Allow columns not in schema |
| `auto_register` | `bool` | `True` | Auto-register if entity doesn't exist |
| `primary_keys` | `List[str]` | `None` | Override inferred primary keys |
| `partition_columns` | `List[str]` | `None` | Override inferred partition columns |
| `storage_base_path` | `str` | `None` | Override inferred storage path |

**Example:**
```python
df = spark.createDataFrame(
    [(1, "2024-01-01", 25, 0.8)],
    ["user_id", "dt", "age", "score"]
)

# Auto-registers on first write
client.write_entity(
    df, EntityKind.FEATURE_VIEW, "user_features", "v1",
    primary_keys=["user_id"],
    partition_columns=["dt"],
    storage_base_path="gs://bucket/features/user_features/v1",
)
```

Auto-register inference:
- `primary_keys` defaults to columns ending in `_id` or named `id`
- `partition_columns` defaults to first match of `dt`, `partition_date`, `feature_month`, `label_month`
- `storage_base_path` defaults to `{registry_parent}/{kind}s/{name}/{version}`

To disable auto-register:
```python
client.write_entity(df, EntityKind.FEATURE_VIEW, "strict_fv", "v1",
                    auto_register=False)
# raises EntityNotFoundError if not pre-registered
```

#### `write_dataset(dataset, name, version, ...) -> None`

Delegates to `write_entity` with `EntityKind.DATASET`. Supports same
`auto_register` and metadata params.

#### `write_training_set(dataset, name, version, split, ...) -> None`

Writes a training/validation/test split. Appends `/{split}` to the storage path.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `split` | `str` | `"train"` | Split name: `"train"`, `"validation"`, `"test"` |

**Example:**
```python
client.write_training_set(train_df, "churn_training", "v1", split="train")
client.write_training_set(val_df, "churn_training", "v1", split="validation")
```

---

### Read

#### `get_entity(kind, name, version, columns, start_date, end_date) -> DataFrame`

Read entity data with column projection, partition pruning, and ML-friendly
type coercion (Decimal→double, Long→int).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kind` | `EntityKind` | — | Entity type |
| `name` | `str` | — | Entity name |
| `version` | `str` | — | Entity version |
| `columns` | `List[str] \| "*"` | `"*"` | Columns to read (pk + partition cols always included) |
| `start_date` | `str` | `None` | Partition filter start (YYYY-MM-DD) |
| `end_date` | `str` | `None` | Partition filter end (YYYY-MM-DD) |

**Example:**
```python
features = client.get_entity(
    EntityKind.FEATURE_VIEW, "user_features", "v1",
    columns=["age", "score"],
    start_date="2024-01-01", end_date="2024-01-31",
)
```

#### `get_dataset(name, version, start_date, end_date) -> DataFrame`

Shorthand for `get_entity(EntityKind.DATASET, ...)`.

#### `get_training_set(name, version, split, start_date, end_date) -> DataFrame`

Read a specific split. Appends `/{split}` to the storage path.

```python
train = client.get_training_set("churn_training", "v1", split="train")
```

---

### Model Feature Assembly

#### `get_model_features(model_name, model_version, query_df, start_date, end_date, checkpoint_interval, checkpoint_dir, checkpoint_ctx) -> DataFrame`

Assemble a wide feature table by joining all feature views that back a
registered model. Automatically handles join key resolution from the model
config.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_name` | `str` | — | Registered model name |
| `model_version` | `str` | — | Registered model version |
| `query_df` | `DataFrame` | — | Query DataFrame containing primary keys + partition column |
| `start_date` | `str` | — | Feature data start date |
| `end_date` | `str` | `None` | Feature data end date |
| `checkpoint_interval` | `int` | `5` | Joins between Spark checkpoints |
| `checkpoint_dir` | `str` | `None` | Checkpoint directory (legacy mode) |
| `checkpoint_ctx` | `CheckpointContext` | `None` | Shared checkpoint context (preferred) |

**Example — standalone:**
```python
query = spark.createDataFrame([(1, "2024-01-01")], ["user_id", "dt"])

features = client.get_model_features(
    "churn_model", "v1", query,
    start_date="2024-01-01",
    checkpoint_dir="/tmp/checkpoints",
)
```

---

### Training Dataset Export

#### `export_training_dataset(query_df, model_name, model_version, label_name, label_version, feature_start_date, feature_end_date, lookback, join_type, dry_run, output_path, checkpoint_ctx) -> DataFrame`

Point-in-time join of features and labels. Supports configurable lookback
for label time-shift alignment.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query_df` | `DataFrame` | — | Query DataFrame |
| `model_name` | `str` | — | Model name |
| `model_version` | `str` | — | Model version |
| `label_name` | `str` | — | Label name |
| `label_version` | `str` | — | Label version |
| `feature_start_date` | `str` | — | Feature data start |
| `feature_end_date` | `str` | `None` | Feature data end |
| `lookback` | `Lookback` | `Lookback("day", 1)` | Label time shift |
| `join_type` | `str` | `"left"` | Join type |
| `dry_run` | `bool` | `True` | If False + output_path set, materialize to parquet |
| `output_path` | `str` | `None` | Output parquet path |
| `checkpoint_ctx` | `CheckpointContext` | `None` | Shared checkpoint context |

**Example — dry run:**
```python
from feature_store import Lookback

dataset = client.export_training_dataset(
    query_df=query,
    model_name="churn_model", model_version="v1",
    label_name="churn_label", label_version="v1",
    feature_start_date="2024-01-01",
    lookback=Lookback(freq="month", periods=1),
)
```

**Example — materialize with checkpoint context:**
```python
with client.checkpoint_context("/tmp/ckpt") as ctx:
    dataset = client.export_training_dataset(
        query_df=query,
        model_name="churn_model", model_version="v1",
        label_name="churn_label", label_version="v1",
        feature_start_date="2024-01-01",
        checkpoint_ctx=ctx,
        dry_run=False,
        output_path="gs://bucket/datasets/churn/v1",
    )
# checkpoint cleaned after all Spark actions complete
```

---

### Checkpoint Management

#### `checkpoint_context(base_dir) -> CheckpointContext`

Context manager that creates an isolated UUID-based checkpoint directory,
configures Spark's checkpoint dir, and guarantees cleanup on exit.

| Parameter | Type | Description |
|-----------|------|-------------|
| `base_dir` | `str` | Parent directory for checkpoint subdirectory |

**Example:**
```python
with client.checkpoint_context("/tmp/checkpoints") as ctx:
    features = client.get_model_features(..., checkpoint_ctx=ctx)
    dataset = client.export_training_dataset(..., checkpoint_ctx=ctx, dry_run=False, output_path="...")
# checkpoint cleaned here — after all Spark writes complete
```

---

### Management

#### `list_entities(kind=None) -> List[Dict]`

List registered entities, optionally filtered by kind.

```python
fvs = client.list_entities(kind=EntityKind.FEATURE_VIEW)
for fv in fvs:
    print(fv["name"], fv["version"])
```

#### `get_entity_info(kind, name, version) -> Config`

Return the typed config object for a registered entity.

```python
config = client.get_entity_info(EntityKind.FEATURE_VIEW, "user_features", "v1")
print(config.primary_keys[0].name)  # "user_id"
```

#### `sync_lifecycle() -> Dict[str, int]`

Scan all registry YAMLs, extract retention policies, and apply GCS lifecycle
rules to the corresponding buckets.

```python
result = client.sync_lifecycle()
# {"bucket-name": 6}  — 6 rules applied
```

#### `build_dependency_graph(name, version) -> Dict[str, List[str]]`

Recursive BFS over entity dependencies. Returns adjacency dict.

```python
graph = client.build_dependency_graph("churn_model", "v1")
# {"churn_model/v1": ["user_fv/v1", "base_model/v2"],
#  "user_fv/v1": [],
#  "base_model/v2": ["raw_features/v1"],
#  ...}
```

#### `migrate_registry(dry_run=True) -> List[str]`

Scan all YAMLs, detect old format (missing `kind`), convert to new format.

```python
# Preview changes
changes = client.migrate_registry(dry_run=True)
print(f"Files to migrate: {changes}")

# Apply migration
migrated = client.migrate_registry(dry_run=False)
```
````

- [ ] **Step 2: Commit**

```bash
cd D:/feature_store && git add docs/API.md && git commit -m "docs: add complete API reference with client usage examples"
```

---

### Task 4: docs/CI_CD.md

**Files:**
- Create: `docs/CI_CD.md`

- [ ] **Step 1: Write docs/CI_CD.md**

````markdown
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
┌──────┐   ┌───────────┐   ┌──────┐   ┌───────┐   ┌─────────┐
│ Lint │ → │ Typecheck │ → │ Test │ → │ Build │ → │ Release │  (tag only)
└──────┘   └───────────┘   └──────┘   └───────┘   └─────────┘
 ruff         pyright        pytest     build.py   GitHub Release
                              3.10/3.11  wheel+zip  + GCS upload
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

See [Build Script](#build-script) below for implementation details.

### Release

Triggered by tags matching `v*`. Stages:

1. Downloads build artifacts from the `build` job
2. Creates a GitHub Release with auto-generated release notes from commits
3. Uploads `dist/*` assets to the release
4. Copies wheel and zip to GCS module path (`$GCS_MODULE_PATH/`)

## Required Secrets

| Secret | Purpose |
|--------|---------|
| `GCS_MODULE_PATH` | GCS path for artifacts (e.g. `gs://bucket/modules/`) |
| `GCP_SA_KEY` | (If not using Workload Identity) Service account key for GCS access |

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

Located at `.github/workflows/ci_cd.yml`. See that file for the complete
workflow definition.

## Environment Requirements

| Component | Version |
|-----------|---------|
| Python | 3.10, 3.11 |
| Java | 8 (Temurin) |
| PySpark | ≥3.3 |
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
````

- [ ] **Step 2: Commit**

```bash
cd D:/feature_store && git add docs/CI_CD.md && git commit -m "docs: add CI/CD pipeline specification"
```

---

### Task 5: scripts/build.py

**Files:**
- Create: `scripts/build.py`

- [ ] **Step 1: Create scripts directory**

```bash
mkdir -p D:/feature_store/scripts
```

- [ ] **Step 2: Write scripts/build.py**

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


def main():
    clean_dist()
    build_wheel()
    zip_path = build_zip()
    for f in sorted(os.listdir(DIST)):
        size = os.path.getsize(os.path.join(DIST, f))
        print(f"  dist/{f} ({size:,} bytes)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Test build script**

```bash
cd D:/feature_store && python scripts/build.py
```
Expected: two files in `dist/` — `.whl` and `.zip`

- [ ] **Step 4: Commit**

```bash
cd D:/feature_store && git add scripts/build.py && git commit -m "feat: add build script producing wheel and zip artifacts"
```

---

### Task 6: .github/workflows/ci_cd.yml

**Files:**
- Create: `.github/workflows/ci_cd.yml`

- [ ] **Step 1: Create directories**

```bash
mkdir -p D:/feature_store/.github/workflows
```

- [ ] **Step 2: Write .github/workflows/ci_cd.yml**

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
        with:
          python-version: "3.10"
      - run: pip install ruff
      - run: ruff check src/ tests/

  # ── Type Check ────────────────────────────────────────
  typecheck:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
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
        with:
          python-version: ${{ matrix.python-version }}
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "8"
      - run: pip install -e ".[dev]"
      - run: PYTHONPATH=src python -m pytest tests/ -v --tb=short

  # ── Build ─────────────────────────────────────────────
  build:
    needs: [test]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
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
        with:
          name: dist
          path: dist/
      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}
      - name: Set up gcloud
        uses: google-github-actions/setup-gcloud@v2
      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: dist/*
          generate_release_notes: true
      - name: Upload to GCS
        run: |
          WHEEL=$(ls dist/*.whl | head -1)
          ZIP=$(ls dist/*.zip | head -1)
          gcloud storage cp "$WHEEL" "$ZIP" "${{ secrets.GCS_MODULE_PATH }}/"
```

- [ ] **Step 3: Add .gitignore entry for dist/**

```bash
cd D:/feature_store && echo "dist/" >> .gitignore
```

- [ ] **Step 4: Commit**

```bash
cd D:/feature_store && git add .github/workflows/ci_cd.yml .gitignore && git commit -m "ci: add GitHub Actions pipeline with lint, typecheck, test, build, and release"
```

---

### Task 7: Final Verification

- [ ] **Step 1: Verify all files exist**

```bash
cd D:/feature_store && ls README.md CHANGELOG.md docs/API.md docs/CI_CD.md scripts/build.py .github/workflows/ci_cd.yml
```

- [ ] **Step 2: Verify build script runs**

```bash
cd D:/feature_store && python scripts/build.py
```

- [ ] **Step 3: Verify all tests still pass**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v --tb=short
```

- [ ] **Step 4: Verify git log**

```bash
cd D:/feature_store && git log --oneline -7
```
