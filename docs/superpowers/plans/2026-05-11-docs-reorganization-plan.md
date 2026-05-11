# Docs Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `docs/API.md` with 6 new structured docs (overview, API index, 5 per-module detail files) plus update README links.

**Architecture:** Six new markdown files in `docs/`. Content sourced from existing `docs/API.md` and source code modules. Each file is self-contained with cross-references. No code changes — documentation only.

**Tech Stack:** Markdown, git

---

### Task 1: Delete old API.md

**Files:**
- Delete: `docs/API.md`

- [ ] **Step 1: Remove docs/API.md**

```bash
git rm docs/API.md
```

- [ ] **Step 2: Verify the file is gone**

```bash
ls docs/API.md 2>&1
```
Expected: "No such file or directory"

- [ ] **Step 3: Commit**

```bash
git commit -m "docs: remove monolithic API.md in favor of structured docs"
```

---

### Task 2: Create project overview (`docs/overview.md`)

**Files:**
- Create: `docs/overview.md`

- [ ] **Step 1: Write docs/overview.md**

```markdown
# Feature Store — Project Overview

## Purpose

A PySpark-native Feature Store client that provides a unified YAML-based registry
for managing machine learning feature pipelines. It standardizes how features,
models, labels, datasets, and training sets are registered, stored, validated,
and assembled — ensuring consistency across ML workflows.

## Project Structure

```
feature_store/
├── types.py       → EntityKind, StorageFormat, UpdateFrequency enums
├── schema.py      → ColumnSpec, KeySpec, config dataclasses, Lookback
├── storage.py     → StorageBackend (ABC), LocalBackend, GCSBackend, get_backend
├── registry.py    → YAML serialization, validation, lifecycle, migration helpers
└── client.py      → FeatureStoreClient (single user-facing entry point)
tests/
├── test_types.py
├── test_schema.py
├── test_storage.py
├── test_registry.py
├── test_client.py
├── test_migration.py
└── conftest.py
docs/
├── overview.md             ← this file
├── api_index.md            ← API catalog
├── api_types.md            ← types module detail
├── api_schema.md           ← schema module detail
├── api_storage.md          ← storage module detail
├── api_registry.md         ← registry module detail
├── api_client.md           ← client module detail
├── CI_CD.md                ← CI/CD pipeline docs
└── examples/               ← YAML config examples
```

## Capabilities

- **Five entity types** — feature_view, model, label, dataset, training_set
- **YAML registry** — typed, versioned, kind-based entity metadata stored as YAML
- **Pluggable storage** — local filesystem and GCS backends, with S3 extensibility
- **Auto-registration** — first write infers and registers entity metadata automatically
- **Schema validation** — DataFrame columns validated against registered config on write
- **Feature assembly** — multi-view joins orchestrated by a model config, with Spark checkpointing
- **Training set export** — point-in-time join of features and labels with configurable lookback
- **Dependency graph** — recursive BFS traversal over entity relationships
- **Lifecycle management** — sync TTL/cold-tier retention rules to GCS bucket policies
- **Migration tooling** — convert legacy YAML format to current schema

## How It Works

### Registration → Storage → Assembly pipeline

```
1. REGISTER       2. WRITE             3. READ               4. ASSEMBLE
   ┌──────┐         ┌──────┐             ┌──────┐              ┌──────────┐
   │Config│   or    │  DF  │──validate──▶│  DF  │              │ query_df │
   │ (DF) │──YAML──▶│ (FV) │             │ (FV) │──join────────│  model   │
   └──────┘         └──────┘             └──────┘   with       │ features │
       │                │                    │     query_df    └──────────┘
       ▼                ▼                    ▼                    │
   registry/        storage/             storage/                 ▼
   feature_view/    feature_views/       feature_views/     ┌──────────┐
   fv_v1.yaml       fv/v1/*.parquet      fv/v1/*.parquet    │ labels   │
                                                            │  join    │
                                                            └──────────┘
                                                                 │
                                                                 ▼
                                                          training_set
```

1. **Register** — A config dataclass or DataFrame defines an entity's schema, keys,
   partitions, and storage location. The config is serialized to YAML in the registry.
2. **Write** — DataFrames are validated against their registered schema and persisted
   as Parquet. Auto-registration handles the first write transparently.
3. **Read** — Entities are read back with column projection, date filtering, and
   automatic type coercion (Decimal→double, Long→int) for ML compatibility.
4. **Assemble** — A model config declares which feature view columns it needs.
   `get_model_features` joins them all against a query DataFrame. `export_training_dataset`
   adds labels with point-in-time correct lookback.

## Installation

```bash
# From source (development)
pip install -e ".[dev]"

# From wheel (cluster deployment)
pip install dist/feature_store-2.0.0-py3-none-any.whl

# As --py-files (spark-submit)
spark-submit --py-files dist/feature_store.zip ...
```

## Quick Start

```python
from pyspark.sql import SparkSession
from feature_store import FeatureStoreClient, EntityKind

spark = SparkSession.builder.appName("demo").getOrCreate()
client = FeatureStoreClient(spark, registry_dir="gs://bucket/registry")

# Write and auto-register a feature view
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

# Read it back
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
| [API Index](api_index.md) | Catalog of all APIs by module |
| [Types Module](api_types.md) | Enums: EntityKind, StorageFormat, UpdateFrequency |
| [Schema Module](api_schema.md) | Config dataclasses: FeatureViewConfig, ModelConfig, etc. |
| [Storage Module](api_storage.md) | Backend abstraction: LocalBackend, GCSBackend |
| [Registry Module](api_registry.md) | YAML serialization, validation, lifecycle |
| [Client Module](api_client.md) | FeatureStoreClient — user-facing API |
| [CI/CD](CI_CD.md) | CI/CD pipeline |
```

- [ ] **Step 2: Verify file contents**

```bash
wc -l docs/overview.md
```
Expected: >100 lines

- [ ] **Step 3: Commit**

```bash
git add docs/overview.md
git commit -m "docs: add project overview"
```

---

### Task 3: Create API catalog (`docs/api_index.md`)

**Files:**
- Create: `docs/api_index.md`

- [ ] **Step 1: Write docs/api_index.md**

```markdown
# API Index

Catalog of all public APIs in the Feature Store, organized by module.

## Modules

| Module | Source | Detail Doc | Description |
|--------|--------|------------|-------------|
| `types` | `feature_store/types.py` | [api_types.md](api_types.md) | Enums for entity kind, storage format, update frequency |
| `schema` | `feature_store/schema.py` | [api_schema.md](api_schema.md) | Typed dataclasses for entity configuration |
| `storage` | `feature_store/storage.py` | [api_storage.md](api_storage.md) | Pluggable storage backends (local, GCS) |
| `registry` | `feature_store/registry.py` | [api_registry.md](api_registry.md) | YAML serialization, schema validation, lifecycle |
| `client` | `feature_store/client.py` | [api_client.md](api_client.md) | FeatureStoreClient — primary user-facing API |

## API Reference by Module

### types

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `EntityKind` | Enum | [api_types.md](api_types.md) | Entity type: feature_view, model, label, dataset, training_set |
| `StorageFormat` | Enum | [api_types.md](api_types.md) | Storage format: parquet |
| `UpdateFrequency` | Enum | [api_types.md](api_types.md) | Update cadence: daily, weekly, monthly |

### schema

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `ColumnSpec` | Dataclass | [api_schema.md](api_schema.md) | Schema column descriptor with optional feature view lineage |
| `KeySpec` | Dataclass | [api_schema.md](api_schema.md) | Primary key or partition column descriptor |
| `Dependency` | Dataclass | [api_schema.md](api_schema.md) | Upstream entity dependency reference |
| `PipelineSpec` | Dataclass | [api_schema.md](api_schema.md) | Pipeline execution and alerting parameters |
| `RetentionSpec` | Dataclass | [api_schema.md](api_schema.md) | Data retention TTL and cold tier settings |
| `StorageSpec` | Dataclass | [api_schema.md](api_schema.md) | Physical storage location and format |
| `Lookback` | Dataclass | [api_schema.md](api_schema.md) | Time window for feature-label point-in-time joins |
| `FeatureViewConfig` | Dataclass | [api_schema.md](api_schema.md) | Feature view entity configuration |
| `ModelConfig` | Dataclass | [api_schema.md](api_schema.md) | Model entity configuration with feature view references |
| `LabelConfig` | Dataclass | [api_schema.md](api_schema.md) | Label entity configuration |
| `DatasetConfig` | Dataclass | [api_schema.md](api_schema.md) | Dataset entity configuration |
| `TrainingSetConfig` | Dataclass | [api_schema.md](api_schema.md) | Alias for DatasetConfig |

### storage

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `StorageBackend` | ABC | [api_storage.md](api_storage.md) | Abstract storage interface |
| `LocalBackend` | Class | [api_storage.md](api_storage.md) | Local filesystem storage via pyarrow/pandas |
| `GCSBackend` | Class | [api_storage.md](api_storage.md) | Google Cloud Storage via Spark native + fsspec |
| `get_backend` | Function | [api_storage.md](api_storage.md) | Factory: returns appropriate backend for a path |

### registry

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `config_to_yaml` | Function | [api_registry.md](api_registry.md) | Serialize a config dataclass to YAML string |
| `yaml_to_config` | Function | [api_registry.md](api_registry.md) | Deserialize YAML dict to typed config object |
| `load_yaml` | Function | [api_registry.md](api_registry.md) | Load and parse a YAML file from a backend |
| `write_yaml` | Function | [api_registry.md](api_registry.md) | Serialize and write config as YAML via a backend |
| `validate_dataframe` | Function | [api_registry.md](api_registry.md) | Validate DataFrame columns against a config |
| `build_config_from_df` | Function | [api_registry.md](api_registry.md) | Infer a typed config from a Spark DataFrame schema |
| `build_model_config` | Function | [api_registry.md](api_registry.md) | Build a ModelConfig from feature references |
| `list_entities` | Function | [api_registry.md](api_registry.md) | Scan registry directory for entity YAML files |
| `sync_lifecycle` | Function | [api_registry.md](api_registry.md) | Apply GCS lifecycle rules from retention configs |

### client

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `FeatureStoreClient` | Class | [api_client.md](api_client.md) | Main entry point for all feature store operations |
| `CheckpointContext` | Class | [api_client.md](api_client.md) | Context manager for Spark checkpoint lifecycle |
| `FeatureStoreClient.register` | Method | [api_client.md](api_client.md) | Register an entity from config or DataFrame |
| `FeatureStoreClient.register_feature_view` | Method | [api_client.md](api_client.md) | Convenience: register a feature view |
| `FeatureStoreClient.register_model` | Method | [api_client.md](api_client.md) | Convenience: register a model |
| `FeatureStoreClient.register_label` | Method | [api_client.md](api_client.md) | Convenience: register a label |
| `FeatureStoreClient.register_dataset` | Method | [api_client.md](api_client.md) | Convenience: register a dataset |
| `FeatureStoreClient.register_training_set` | Method | [api_client.md](api_client.md) | Convenience: register a training set |
| `FeatureStoreClient.write_entity` | Method | [api_client.md](api_client.md) | Validate and persist a DataFrame |
| `FeatureStoreClient.get_entity` | Method | [api_client.md](api_client.md) | Read entity data with filters and type coercion |
| `FeatureStoreClient.get_model_features` | Method | [api_client.md](api_client.md) | Assemble feature DataFrame from model config |
| `FeatureStoreClient.export_training_dataset` | Method | [api_client.md](api_client.md) | Point-in-time join of features and labels |
| `FeatureStoreClient.write_dataset` | Method | [api_client.md](api_client.md) | Write a dataset entity |
| `FeatureStoreClient.get_dataset` | Method | [api_client.md](api_client.md) | Read a dataset entity |
| `FeatureStoreClient.write_training_set` | Method | [api_client.md](api_client.md) | Write a train/val/test split |
| `FeatureStoreClient.get_training_set` | Method | [api_client.md](api_client.md) | Read a train/val/test split |
| `FeatureStoreClient.checkpoint_context` | Method | [api_client.md](api_client.md) | Context manager for checkpoint directory |
| `FeatureStoreClient.list_entities` | Method | [api_client.md](api_client.md) | List registered entities |
| `FeatureStoreClient.get_entity_info` | Method | [api_client.md](api_client.md) | Return typed config for a registered entity |
| `FeatureStoreClient.sync_lifecycle` | Method | [api_client.md](api_client.md) | Apply GCS lifecycle rules |
| `FeatureStoreClient.build_dependency_graph` | Method | [api_client.md](api_client.md) | BFS traversal of entity dependencies |
| `FeatureStoreClient.migrate_registry` | Method | [api_client.md](api_client.md) | Convert legacy YAML to current schema |

## Exception Hierarchy

| Exception | Module | Parent | Description |
|-----------|--------|--------|-------------|
| `FeatureStoreError` | registry | `Exception` | Base exception for all feature store errors |
| `EntityNotFoundError` | registry | `FeatureStoreError` | Requested entity not in registry |
| `ColumnNotFoundError` | registry | `FeatureStoreError` | Requested column not found |
| `SchemaValidationError` | registry | `FeatureStoreError` | Schema validation failure |
| `MissingColumnsError` | registry | `SchemaValidationError` | DataFrame missing required columns |
| `ExtraColumnsError` | registry | `SchemaValidationError` | DataFrame has undeclared columns |
| `RegistryFormatError` | registry | `FeatureStoreError` | Malformed registry content |
| `StorageError` | registry | `FeatureStoreError` | Storage-level operation failure |
```

- [ ] **Step 2: Verify file**

```bash
wc -l docs/api_index.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/api_index.md
git commit -m "docs: add API index catalog"
```

---

### Task 4: Create types module doc (`docs/api_types.md`)

**Files:**
- Create: `docs/api_types.md`

- [ ] **Step 1: Write docs/api_types.md**

```markdown
# Module: `feature_store.types`

Enums used across all feature store modules to represent entity kinds, storage
formats, and update cadences.

**Source:** `src/feature_store/types.py`

---

## Public Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `EntityKind` | Enum | Types of entities managed by the feature store |
| `StorageFormat` | Enum | Supported file formats for data persistence |
| `UpdateFrequency` | Enum | Update cadences for feature pipelines |

---

## Detailed API

### `EntityKind`

```python
class EntityKind(enum.Enum):
    FEATURE_VIEW = "feature_view"
    MODEL        = "model"
    LABEL        = "label"
    DATASET      = "dataset"
    TRAINING_SET = "training_set"
```

Identifies the type of an entity in the feature store registry. Used as the
`kind` key in registry YAMLs and to select the correct config dataclass during
deserialization.

| Member | Value | Description |
|--------|-------|-------------|
| `FEATURE_VIEW` | `"feature_view"` | A table of computed features with a pipeline |
| `MODEL` | `"model"` | A model whose schema references feature view columns |
| `LABEL` | `"label"` | Ground-truth labels for supervised learning |
| `DATASET` | `"dataset"` | A generic dataset without schema |
| `TRAINING_SET` | `"training_set"` | A train/val/test split (alias for DatasetConfig) |

**Usage:**

```python
from feature_store import EntityKind

client.register(df, kind=EntityKind.FEATURE_VIEW, name="my_fv", version="v1",
                primary_keys=["id"], partition_columns=["dt"],
                storage_base_path="gs://bucket/features/my_fv/v1")
```

---

### `StorageFormat`

```python
class StorageFormat(enum.Enum):
    PARQUET = "parquet"
```

Supported file formats for data persistence. Currently only Parquet.

| Member | Value | Description |
|--------|-------|-------------|
| `PARQUET` | `"parquet"` | Apache Parquet columnar format |

---

### `UpdateFrequency`

```python
class UpdateFrequency(enum.Enum):
    DAILY   = "daily"
    WEEKLY  = "weekly"
    MONTHLY = "monthly"
```

Update cadence for feature pipelines. Used in `PipelineSpec.update_frequency`
to control scheduling and alert thresholds.

| Member | Value | Description |
|--------|-------|-------------|
| `DAILY` | `"daily"` | Updated every day |
| `WEEKLY` | `"weekly"` | Updated every week |
| `MONTHLY` | `"monthly"` | Updated every month |

**Usage:**

```python
from feature_store import FeatureViewConfig, PipelineSpec, UpdateFrequency

config = FeatureViewConfig(
    name="user_features", version="v1",
    primary_keys=[...], partition_columns=[...],
    pipeline=PipelineSpec(update_frequency=UpdateFrequency.DAILY.value),
)
```

---

## Cross-References

- [Schema Module](api_schema.md) — config dataclasses that use these enums
- [Client Module](api_client.md) — entity registration requiring `EntityKind`
```

- [ ] **Step 2: Verify file**

```bash
wc -l docs/api_types.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/api_types.md
git commit -m "docs: add types module API reference"
```

---

### Task 5: Create schema module doc (`docs/api_schema.md`)

**Files:**
- Create: `docs/api_schema.md`

- [ ] **Step 1: Write docs/api_schema.md**

```markdown
# Module: `feature_store.schema`

Typed configuration dataclasses for defining feature store entities. All config
classes extend `_BaseConfig` which provides shared fields (name, version, owner,
description, dependencies, keys) and name validation.

**Source:** `src/feature_store/schema.py`

---

## Public Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `ColumnSpec` | Dataclass | Describes a single column in an entity schema |
| `KeySpec` | Dataclass | Describes a primary key or partition column |
| `Dependency` | Dataclass | Reference to an upstream entity |
| `PipelineSpec` | Dataclass | Pipeline execution and alerting parameters |
| `RetentionSpec` | Dataclass | Data retention TTL and cold-tier settings |
| `StorageSpec` | Dataclass | Physical storage location and format |
| `Lookback` | Dataclass | Time window for point-in-time label join |
| `FeatureViewConfig` | Dataclass | Feature view entity configuration |
| `ModelConfig` | Dataclass | Model entity configuration |
| `LabelConfig` | Dataclass | Label entity configuration |
| `DatasetConfig` | Dataclass | Dataset entity configuration |
| `TrainingSetConfig` | Dataclass | Alias for `DatasetConfig` |

---

## Core Specifications

### `ColumnSpec`

```python
@dataclass
class ColumnSpec:
    name: str
    type: str
    is_label: bool = False
    feature_view: Optional[str] = None
    feature_view_version: Optional[str] = None
```

Describes a single column in a feature view, model, or label schema.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | Required | Column name |
| `type` | `str` | Required | Data type (e.g. `"string"`, `"integer"`, `"double"`) |
| `is_label` | `bool` | `False` | Whether this column is a label |
| `feature_view` | `Optional[str]` | `None` | Source feature view name (required for model columns) |
| `feature_view_version` | `Optional[str]` | `None` | Source feature view version (required for model columns) |

**Usage:**

```python
from feature_store import ColumnSpec

col = ColumnSpec(name="age", type="integer")
model_col = ColumnSpec(
    name="age", type="integer",
    feature_view="user_fv", feature_view_version="v1",
)
```

---

### `KeySpec`

```python
@dataclass
class KeySpec:
    name: str
    type: str
    format: Optional[str] = None
```

Describes a primary key or partition column.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | Required | Column name |
| `type` | `str` | Required | Data type |
| `format` | `Optional[str]` | `None` | Optional format string (e.g. `"yyyy-MM-dd"` for dates) |

**Usage:**

```python
from feature_store import KeySpec

pk = KeySpec(name="user_id", type="string")
pc = KeySpec(name="dt", type="string", format="yyyy-MM-dd")
```

---

### `Dependency`

```python
@dataclass
class Dependency:
    name: str
    version: str
```

A reference to an upstream entity that this entity depends on.

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Name of the dependency |
| `version` | `str` | Version of the dependency |

---

### `PipelineSpec`

```python
@dataclass
class PipelineSpec:
    update_frequency: str = "daily"
    source_job: str = ""
    alert_threshold_hours: int = 48
```

Pipeline execution parameters for feature views.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `update_frequency` | `str` | `"daily"` | How often the pipeline runs (`"daily"`, `"weekly"`, `"monthly"`) |
| `source_job` | `str` | `""` | Identifier of the source job |
| `alert_threshold_hours` | `int` | `48` | Hours after expected completion before alerting |

---

### `RetentionSpec`

```python
@dataclass
class RetentionSpec:
    ttl_days: int = 360
    cold_tier_days: int = 180
```

Data retention policy.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ttl_days` | `int` | `360` | Days before data is eligible for deletion |
| `cold_tier_days` | `int` | `180` | Days before data moves to cold storage |

---

### `StorageSpec`

```python
@dataclass
class StorageSpec:
    base_path: str
    format: str = "parquet"
```

Physical storage location for an entity's data.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_path` | `str` | Required | Base path (local or `gs://` for GCS) |
| `format` | `str` | `"parquet"` | Storage format |

---

### `Lookback`

```python
@dataclass
class Lookback:
    freq: str = "day"
    periods: int = 1
```

Controls the time shift applied to label dates during training set export.
Labels are shifted back by `periods` `freq` units so they align with
earlier feature dates for point-in-time correct joins.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `freq` | `str` | `"day"` | Unit: `"day"` or `"month"` |
| `periods` | `int` | `1` | Number of freq units to shift |

Validation: `freq` must be `"day"` or `"month"` (raises `ValueError`).

**Usage:**

```python
from feature_store import Lookback

# Features from Jan 1 → labels from Jan 2 (1 day lookback)
lb = Lookback(freq="day", periods=1)

# Features from Jan 1 → labels from Feb 1 (1 month lookback)
lb = Lookback(freq="month", periods=1)
```

---

## Entity Config Classes

All config classes share these base fields from `_BaseConfig`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | Required | Entity name. Must match `[a-z][a-z0-9_]*` |
| `version` | `str` | Required | Entity version (e.g. `"v1"`) |
| `owner` | `str` | `""` | Owner or team |
| `description` | `str` | `""` | Human-readable description |
| `dependency` | `List[Dependency]` | `[]` | Upstream dependencies |
| `primary_keys` | `List[KeySpec]` | `[]` | Primary key columns |
| `partition_columns` | `List[KeySpec]` | `[]` | Partition columns |

Validation in `__post_init__`:
- `name` must match `^[a-z][a-z0-9_]*$`
- Primary keys, partition columns, and schema columns must not overlap

---

### `FeatureViewConfig`

```python
@dataclass
class FeatureViewConfig(_BaseConfig):
    storage: Optional[StorageSpec] = None
    pipeline: PipelineSpec = field(default_factory=PipelineSpec)
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "feature_view"
```

Configuration for a feature view entity.

Additional constraints: `primary_keys` and `partition_columns` must not be empty.

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
    owner="ml-team",
    description="User demographic and behavioral features",
)
client.register(config)
```

---

### `ModelConfig`

```python
@dataclass
class ModelConfig(_BaseConfig):
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "model"
```

Configuration for a model entity. Each column in `schema` must declare a
`feature_view` and `feature_view_version`, linking it to a specific feature
view column.

**Usage:**

```python
from feature_store import ModelConfig, KeySpec, ColumnSpec

config = ModelConfig(
    name="churn_model", version="v1",
    primary_keys=[KeySpec(name="user_id", type="string")],
    partition_columns=[KeySpec(name="dt", type="string")],
    schema=[
        ColumnSpec(name="age", type="integer",
                   feature_view="user_fv", feature_view_version="v1"),
        ColumnSpec(name="score", type="double",
                   feature_view="user_fv", feature_view_version="v1"),
    ],
)
client.register(config)
```

---

### `LabelConfig`

```python
@dataclass
class LabelConfig(_BaseConfig):
    storage: Optional[StorageSpec] = None
    schema: List[ColumnSpec] = field(default_factory=list)
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    kind: str = "label"
```

Configuration for a label entity. Labels store ground truth values for
supervised learning and are joined with features during training set export.

**Usage:**

```python
from feature_store import LabelConfig, KeySpec, ColumnSpec, StorageSpec

config = LabelConfig(
    name="churn_label", version="v1",
    primary_keys=[KeySpec(name="user_id", type="string")],
    partition_columns=[KeySpec(name="label_month", type="string")],
    storage=StorageSpec(base_path="gs://bucket/labels/churn/v1"),
    schema=[
        ColumnSpec(name="churned", type="boolean", is_label=True),
    ],
)
client.register(config)
```

---

### `DatasetConfig`

```python
@dataclass
class DatasetConfig(_BaseConfig):
    storage: Optional[StorageSpec] = None
    kind: str = "dataset"
```

Configuration for a generic dataset entity. Datasets have no schema validation
(extra columns are always allowed on write).

---

### `TrainingSetConfig`

```python
TrainingSetConfig = DatasetConfig
```

Alias for `DatasetConfig`. Training sets are written as train/validation/test
splits under a shared entity.

---

## Cross-References

- [Types Module](api_types.md) — enums used by config fields
- [Registry Module](api_registry.md) — YAML serialization and deserialization of configs
- [Client Module](api_client.md) — registration and entity operations using configs
```

- [ ] **Step 2: Verify file**

```bash
wc -l docs/api_schema.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/api_schema.md
git commit -m "docs: add schema module API reference"
```

---

### Task 6: Create storage module doc (`docs/api_storage.md`)

**Files:**
- Create: `docs/api_storage.md`

- [ ] **Step 1: Write docs/api_storage.md**

```markdown
# Module: `feature_store.storage`

Pluggable storage backend abstraction. Provides a common interface for reading
and writing Parquet data, plus filesystem operations (open, exists, glob, copy,
remove). Two implementations are included: local filesystem and Google Cloud
Storage.

**Source:** `src/feature_store/storage.py`

---

## Public Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `StorageBackend` | ABC | Abstract interface for storage operations |
| `LocalBackend` | Class | Local filesystem using pyarrow/pandas |
| `GCSBackend` | Class | GCS using Spark native Parquet + fsspec |
| `get_backend` | Function | Factory: selects backend based on path scheme |

---

## Detailed API

### `StorageBackend` (ABC)

```python
class StorageBackend(abc.ABC):
    def read_parquet(self, spark, path, columns=None, filters=None): ...
    def write_parquet(self, df, path, partition_cols, mode, compression="snappy", partition_num=24): ...
    def open(self, path, mode="r"): ...
    def exists(self, path) -> bool: ...
    def glob(self, pattern) -> List[str]: ...
    def cp(self, src, dst): ...
    def rm(self, path, recursive=False): ...
```

Abstract base class defining the storage interface. All backend implementations
must implement these seven methods.

| Method | Description |
|--------|-------------|
| `read_parquet(spark, path, columns, filters)` | Read a Parquet dataset into a Spark DataFrame |
| `write_parquet(df, path, partition_cols, mode, compression, partition_num)` | Write a DataFrame as Parquet |
| `open(path, mode)` | Open a file for reading/writing text |
| `exists(path)` | Check whether a path exists |
| `glob(pattern)` | List paths matching a glob pattern |
| `cp(src, dst)` | Copy a file or directory |
| `rm(path, recursive)` | Remove a file or directory |

---

### `LocalBackend`

```python
class LocalBackend(StorageBackend):
    ...
```

Backend that reads from and writes to the local filesystem. Uses pyarrow/pandas
for Parquet I/O to avoid Hadoop filesystem issues on Windows.

**Parquet I/O strategy:**
- **Read:** Uses `pyarrow.parquet.read_table` → pandas → Spark DataFrame
- **Write:** Uses `pyarrow.parquet.write_to_dataset` (partitioned) or
  `write_table` (unpartitioned)
- **Modes:** `"overwrite"` (rmtree + write), `"append"` (write alongside),
  `"ignore"` (skip if exists)

**Usage:**

```python
from feature_store import LocalBackend

backend = LocalBackend()
backend.write_parquet(df, "/data/features", partition_cols=["dt"], mode="overwrite")
df = backend.read_parquet(spark, "/data/features", columns=["age", "score"])
```

---

### `GCSBackend`

```python
class GCSBackend(StorageBackend):
    ...
```

Backend that reads from and writes to Google Cloud Storage (`gs://` paths).

**Dependencies:**
- Parquet I/O: Spark native GCS connector (handles `gs://` URIs natively)
- File operations: `fsspec` with `gcsfs` for open, exists, glob, cp, rm
- Lifecycle: `google-cloud-storage` for bucket lifecycle management

**Parquet I/O strategy:**
- **Read:** `spark.read.parquet(path)` — Spark native GCS
- **Write:** `df.write.partitionBy(...).mode(mode).parquet(path)` —
  includes `partitionOverwriteMode=dynamic` for safe partition updates
- **Repartition:** Data is repartitioned to `partition_num` before write

**Usage:**

```python
from feature_store import GCSBackend

backend = GCSBackend()
backend.write_parquet(
    df, "gs://bucket/features/user/v1",
    partition_cols=["dt"], mode="overwrite", partition_num=24,
)
```

---

### `get_backend`

```python
def get_backend(path: str) -> StorageBackend:
```

Factory function that returns the appropriate `StorageBackend` for a given path.

| Path pattern | Backend |
|-------------|---------|
| `gs://...` | `GCSBackend` |
| `s3://...` | raises `NotImplementedError` |
| everything else | `LocalBackend` |

**Usage:**

```python
from feature_store import get_backend

backend = get_backend("gs://bucket/registry")   # → GCSBackend
backend = get_backend("/local/registry")        # → LocalBackend
```

The `FeatureStoreClient` constructor calls this automatically based on
`registry_dir`.

---

## Cross-References

- [Client Module](api_client.md) — uses `get_backend` for constructor
- [Registry Module](api_registry.md) — `load_yaml`/`write_yaml` use backend I/O
```

- [ ] **Step 2: Verify file**

```bash
wc -l docs/api_storage.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/api_storage.md
git commit -m "docs: add storage module API reference"
```

---

### Task 7: Create registry module doc (`docs/api_registry.md`)

**Files:**
- Create: `docs/api_registry.md`

- [ ] **Step 1: Write docs/api_registry.md**

```markdown
# Module: `feature_store.registry`

YAML serialization, DataFrame validation, config inference, and lifecycle
management for the feature store registry. Most functions are used internally
by `FeatureStoreClient` but are available for direct use.

**Source:** `src/feature_store/registry.py`

---

## Public Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `config_to_yaml` | Function | Serialize any config dataclass to a YAML string |
| `yaml_to_config` | Function | Deserialize a YAML dict into a typed config object |
| `load_yaml` | Function | Load and parse a YAML file from a backend |
| `write_yaml` | Function | Serialize and write config as YAML via a backend |
| `validate_dataframe` | Function | Validate DataFrame columns against a config |
| `build_config_from_df` | Function | Infer a typed config from a DataFrame schema |
| `build_model_config` | Function | Build a ModelConfig from feature references |
| `list_entities` | Function | Scan registry directory for entity YAMLs |
| `sync_lifecycle` | Function | Apply GCS lifecycle rules from retention configs |

## Exceptions

| Exception | Parent | Description |
|-----------|--------|-------------|
| `FeatureStoreError` | `Exception` | Base exception |
| `EntityNotFoundError` | `FeatureStoreError` | Entity not found in registry |
| `ColumnNotFoundError` | `FeatureStoreError` | Column not found |
| `SchemaValidationError` | `FeatureStoreError` | Schema validation failed |
| `MissingColumnsError` | `SchemaValidationError` | DataFrame missing required columns |
| `ExtraColumnsError` | `SchemaValidationError` | DataFrame has undeclared columns |
| `RegistryFormatError` | `FeatureStoreError` | Malformed registry content |
| `StorageError` | `FeatureStoreError` | Storage operation failed |

---

## Detailed API

### YAML Serialization

#### `config_to_yaml`

```python
def config_to_yaml(config) -> str:
```

Serialize any config dataclass (`FeatureViewConfig`, `ModelConfig`, `LabelConfig`,
`DatasetConfig`) to a YAML string. Only emits truthy optional fields to keep
YAML concise.

| Parameter | Type | Description |
|-----------|------|-------------|
| `config` | Config dataclass | The typed config to serialize |

Returns a YAML string.

**Usage:**

```python
from feature_store import config_to_yaml, FeatureViewConfig, KeySpec

config = FeatureViewConfig(
    name="fv", version="v1",
    primary_keys=[KeySpec(name="id", type="string")],
    partition_columns=[KeySpec(name="dt", type="string")],
)
yaml_str = config_to_yaml(config)
print(yaml_str)
# kind: feature_view
# name: fv
# version: v1
# primary_keys:
# - name: id
#   type: string
# partition_columns:
# - name: dt
#   type: string
```

---

#### `yaml_to_config`

```python
def yaml_to_config(data: Dict[str, Any]) -> Config:
```

Deserialize a dict (parsed from YAML) back into the appropriate typed config
object. The `kind` key determines which dataclass is instantiated.

| Parameter | Type | Description |
|-----------|------|-------------|
| `data` | `Dict[str, Any]` | Parsed YAML dict (must contain `"kind"`) |

Returns a config dataclass corresponding to `data["kind"]`.

| `kind` value | Return type |
|-------------|-------------|
| `"feature_view"` | `FeatureViewConfig` |
| `"model"` | `ModelConfig` |
| `"label"` | `LabelConfig` |
| `"dataset"` | `DatasetConfig` |
| `"training_set"` | `DatasetConfig` |

Raises `ValueError` if `kind` is missing or unknown.

**Usage:**

```python
from feature_store import yaml_to_config

data = {"kind": "feature_view", "name": "fv", "version": "v1",
        "primary_keys": [{"name": "id", "type": "string"}],
        "partition_columns": [{"name": "dt", "type": "string"}]}
config = yaml_to_config(data)
print(type(config).__name__)  # "FeatureViewConfig"
```

---

#### `load_yaml`

```python
def load_yaml(backend, path: str) -> Dict[str, Any]:
```

Load a YAML file from the given storage backend and return the parsed dict.

| Parameter | Type | Description |
|-----------|------|-------------|
| `backend` | `StorageBackend` | Backend to read from |
| `path` | `str` | Path to the YAML file |

**Usage:**

```python
from feature_store import load_yaml, get_backend

backend = get_backend("gs://bucket/registry")
data = load_yaml(backend, "gs://bucket/registry/feature_view/fv_v1.yaml")
```

---

#### `write_yaml`

```python
def write_yaml(backend, path: str, config) -> str:
```

Serialize `config` to YAML and write it through `backend` at `path`. Creates
parent directories automatically for `LocalBackend`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `backend` | `StorageBackend` | Backend to write through |
| `path` | `str` | Destination path |
| `config` | Config dataclass | Config to serialize and write |

Returns `path` for chaining.

---

### Validation & Inference

#### `validate_dataframe`

```python
def validate_dataframe(df, config, allow_extra_columns: bool = False) -> None:
```

Validate that a Spark DataFrame has the columns declared by a config. Checks
that all primary keys, partition columns, and schema columns (if any) are
present.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | Required | Spark DataFrame to validate |
| `config` | Config dataclass | Required | Config defining expected columns |
| `allow_extra_columns` | `bool` | `False` | If True, extra columns don't raise |

Raises:
- `MissingColumnsError` if any expected column is absent
- `ExtraColumnsError` if extra columns present and `allow_extra_columns=False`

**Usage:**

```python
from feature_store import validate_dataframe

# Raises MissingColumnsError if "age" column is missing
validate_dataframe(df, feature_view_config)
```

---

#### `build_config_from_df`

```python
def build_config_from_df(
    df,
    kind: EntityKind,
    name: str,
    version: str,
    primary_keys: List[str],
    partition_columns: List[str],
    storage_base_path: str,
    owner: str = "",
    description: str = "",
    **kwargs,
) -> Config:
```

Infer a typed config dataclass from a Spark DataFrame schema. Columns listed in
`primary_keys` or `partition_columns` are excluded from the inferred schema.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | Required | Spark DataFrame to infer from |
| `kind` | `EntityKind` | Required | Entity type |
| `name` | `str` | Required | Entity name |
| `version` | `str` | Required | Entity version |
| `primary_keys` | `List[str]` | Required | Primary key column names |
| `partition_columns` | `List[str]` | Required | Partition column names |
| `storage_base_path` | `str` | Required | Storage path |
| `owner` | `str` | `""` | Owner |
| `description` | `str` | `""` | Description |

Returns the appropriate config type for `kind`. Spark types are mapped:
`StringType`→`"string"`, `IntegerType`→`"integer"`, `LongType`→`"long"`,
`DoubleType`→`"double"`, `FloatType`→`"float"`, `BooleanType`→`"boolean"`,
`DateType`→`"date"`, `TimestampType`→`"timestamp"`, `DecimalType`→`"decimal"`.

---

#### `build_model_config`

```python
def build_model_config(
    features: list,
    name: str,
    version: str,
    primary_keys: List[str],
    partition_columns: List[str],
    owner: str = "",
    description: str = "",
    dependency: Optional[List[Dependency]] = None,
) -> ModelConfig:
```

Build a `ModelConfig` from a list of feature references.

`features` can be:
- A list of dicts matching `ColumnSpec` fields
- Legacy URN strings: `"feature_view:name/version/column"`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `features` | `list` | Required | Feature references (dicts or URN strings) |
| `name` | `str` | Required | Model name |
| `version` | `str` | Required | Model version |
| `primary_keys` | `List[str]` | Required | Primary key column names |
| `partition_columns` | `List[str]` | Required | Partition column names |
| `owner` | `str` | `""` | Owner |
| `description` | `str` | `""` | Description |
| `dependency` | `Optional[List[Dependency]]` | `None` | Upstream dependencies |

**Usage:**

```python
from feature_store import build_model_config

config = build_model_config(
    features=[
        {"name": "age", "type": "integer",
         "feature_view": "user_fv", "feature_view_version": "v1"},
        {"name": "score", "type": "double",
         "feature_view": "user_fv", "feature_view_version": "v1"},
    ],
    name="churn_model", version="v1",
    primary_keys=["user_id"], partition_columns=["dt"],
)

# Legacy URN format:
config = build_model_config(
    features=[
        "feature_view:user_fv/v1/age",
        "feature_view:user_fv/v1/score",
    ],
    name="churn_model", version="v1",
    primary_keys=["user_id"], partition_columns=["dt"],
)
```

---

### Management

#### `list_entities`

```python
def list_entities(
    backend,
    registry_dir: str,
    kind: Optional[EntityKind] = None,
) -> List[Dict]:
```

Scan `registry_dir` recursively for `*.yaml` files and return their parsed contents.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `backend` | `StorageBackend` | Required | Backend to scan |
| `registry_dir` | `str` | Required | Registry root directory |
| `kind` | `Optional[EntityKind]` | `None` | Filter by entity kind |

Returns `List[Dict]` where each dict has keys `"path"` and `"data"` (the parsed YAML).

---

#### `sync_lifecycle`

```python
def sync_lifecycle(backend, registry_dir: str) -> Dict[str, int]:
```

Scan all registry YAMLs for entities with both `storage` and `retention`
configuration on GCS. Creates or updates GCS lifecycle rules to delete objects
after `ttl_days`. Replaces existing rules matching the same prefix.

| Parameter | Type | Description |
|-----------|------|-------------|
| `backend` | `StorageBackend` | Backend for registry scanning |
| `registry_dir` | `str` | Registry root directory |

Returns `Dict[str, int]` mapping `"name/version"` → status (`1` = success, `-1` = failure).

Requires `google-cloud-storage` to be installed.

**Usage:**

```python
from feature_store import sync_lifecycle, get_backend

backend = get_backend("gs://bucket/registry")
result = sync_lifecycle(backend, "gs://bucket/registry")
# {"user_features/v1": 1, "churn_label/v1": -1}
```

---

## Cross-References

- [Schema Module](api_schema.md) — config dataclasses serialized/deserialized here
- [Storage Module](api_storage.md) — backend abstraction used for I/O
- [Client Module](api_client.md) — delegates to these functions internally
```

- [ ] **Step 2: Verify file**

```bash
wc -l docs/api_registry.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/api_registry.md
git commit -m "docs: add registry module API reference"
```

---

### Task 8: Create client module doc (`docs/api_client.md`)

**Files:**
- Create: `docs/api_client.md`

- [ ] **Step 1: Write docs/api_client.md**

```markdown
# Module: `feature_store.client`

The primary user-facing API. `FeatureStoreClient` provides a unified interface
for registering, writing, reading, and assembling feature store entities.

**Source:** `src/feature_store/client.py`

---

## Public Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `FeatureStoreClient` | Class | Main entry point for all feature store operations |
| `CheckpointContext` | Class | Context manager for Spark checkpoint lifecycle |

---

## `FeatureStoreClient`

### Constructor

```python
FeatureStoreClient(spark: SparkSession, registry_dir: str)
```

Creates a client backed by the specified Spark session and registry directory.
The storage backend is automatically selected based on the registry path scheme.

| Parameter | Type | Description |
|-----------|------|-------------|
| `spark` | `SparkSession` | Active PySpark session |
| `registry_dir` | `str` | Registry root path. `gs://` for GCS, local path otherwise |

**Usage:**

```python
from pyspark.sql import SparkSession
from feature_store import FeatureStoreClient

spark = SparkSession.builder.appName("fs").getOrCreate()
client = FeatureStoreClient(spark, "gs://my-bucket/feature_store/registry")
```

---

### Registration

#### `register`

```python
def register(
    self,
    source,
    kind: Optional[EntityKind] = None,
    name: Optional[str] = None,
    version: str = "v1",
    primary_keys: Optional[List[str]] = None,
    partition_columns: Optional[List[str]] = None,
    **kwargs,
) -> str:
```

Register an entity from a config dataclass or a Spark DataFrame. When given a
DataFrame, infers the config from the schema.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | `Config \| DataFrame` | Required | Config object or DataFrame |
| `kind` | `Optional[EntityKind]` | `None` | Entity type (required for DataFrame source) |
| `name` | `Optional[str]` | `None` | Entity name (required for DataFrame source) |
| `version` | `str` | `"v1"` | Entity version |
| `primary_keys` | `Optional[List[str]]` | `None` | Primary key column names |
| `partition_columns` | `Optional[List[str]]` | `None` | Partition column names |
| `storage_base_path` | `str` | — | Storage path (kwargs, required for entities with storage) |
| `owner` | `str` | — | Owner (kwargs) |
| `description` | `str` | — | Description (kwargs) |

Returns the YAML registry file path.

**Example — from Config:**

```python
from feature_store import ModelConfig, KeySpec, ColumnSpec

config = ModelConfig(
    name="churn_model", version="v1",
    primary_keys=[KeySpec(name="user_id", type="string")],
    partition_columns=[KeySpec(name="dt", type="string")],
    schema=[
        ColumnSpec(name="age", type="integer",
                   feature_view="user_fv", feature_view_version="v1"),
    ],
)
client.register(config)
```

**Example — from DataFrame:**

```python
df = spark.createDataFrame([(1, "2024-01-01", 25)], ["user_id", "dt", "age"])
client.register(
    df, kind=EntityKind.FEATURE_VIEW, name="user_fv", version="v1",
    primary_keys=["user_id"], partition_columns=["dt"],
    storage_base_path="gs://bucket/features/user_fv/v1",
)
```

---

#### Convenience Registration Methods

Each delegates to `register` with a fixed `kind`:

| Method | Equivalent `kind` |
|--------|-------------------|
| `register_feature_view(source, name, version, primary_keys, partition_columns, **kwargs)` | `EntityKind.FEATURE_VIEW` |
| `register_model(source, name, version, primary_keys, partition_columns, **kwargs)` | `EntityKind.MODEL` |
| `register_label(source, name, version, primary_keys, partition_columns, **kwargs)` | `EntityKind.LABEL` |
| `register_dataset(source, name, version, primary_keys, partition_columns, **kwargs)` | `EntityKind.DATASET` |
| `register_training_set(source, name, version, primary_keys, partition_columns, **kwargs)` | `EntityKind.TRAINING_SET` |

---

### Write Operations

#### `write_entity`

```python
def write_entity(
    self,
    df,
    kind: EntityKind,
    name: str,
    version: str,
    partition_num: int = 24,
    allow_extra_columns: bool = False,
    auto_register: bool = True,
    primary_keys: Optional[List[str]] = None,
    partition_columns: Optional[List[str]] = None,
    storage_base_path: Optional[str] = None,
    **kwargs,
) -> None:
```

Validate a DataFrame against the registered config and persist it as Parquet.
On first write, automatically registers the entity if `auto_register=True`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | Required | Spark DataFrame to write |
| `kind` | `EntityKind` | Required | Entity type |
| `name` | `str` | Required | Entity name |
| `version` | `str` | Required | Entity version |
| `partition_num` | `int` | `24` | Number of output partitions |
| `allow_extra_columns` | `bool` | `False` | Allow columns not in schema |
| `auto_register` | `bool` | `True` | Auto-register if entity doesn't exist |
| `primary_keys` | `Optional[List[str]]` | `None` | Override inferred primary keys |
| `partition_columns` | `Optional[List[str]]` | `None` | Override inferred partition columns |
| `storage_base_path` | `Optional[str]` | `None` | Override inferred storage path |

**Auto-register inference rules (when `auto_register=True`):**
- `primary_keys`: columns ending in `_id` or named `id`
- `partition_columns`: first match of `dt`, `partition_date`, `feature_month`, `label_month`
- `storage_base_path`: `{registry_parent}/{kind}s/{name}/{version}`

**Usage:**

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

# Strict mode — raises EntityNotFoundError if not pre-registered
client.write_entity(
    df, EntityKind.FEATURE_VIEW, "strict_fv", "v1",
    auto_register=False,
)
```

---

#### `write_dataset`

```python
def write_dataset(
    self, dataset, name: str, version: str,
    mode: str = "overwrite", partition_num: int = 200,
    auto_register: bool = True,
    primary_keys: Optional[List[str]] = None,
    partition_columns: Optional[List[str]] = None,
    storage_base_path: Optional[str] = None,
    **kwargs,
) -> None:
```

Write a dataset entity. Delegates to `write_entity` with `EntityKind.DATASET`.

---

#### `write_training_set`

```python
def write_training_set(
    self, dataset, name: str, version: str,
    split: str = "train", mode: str = "overwrite",
    auto_register: bool = True,
    primary_keys: Optional[List[str]] = None,
    partition_columns: Optional[List[str]] = None,
    storage_base_path: Optional[str] = None,
    **kwargs,
) -> None:
```

Write a train/validation/test split. Appends `/{split}` to the storage base path.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `split` | `str` | `"train"` | Split name: `"train"`, `"validation"`, `"test"` |

**Usage:**

```python
client.write_training_set(train_df, "churn_training", "v1", split="train")
client.write_training_set(val_df, "churn_training", "v1", split="validation")
client.write_training_set(test_df, "churn_training", "v1", split="test")
```

---

### Read Operations

#### `get_entity`

```python
def get_entity(
    self,
    kind: EntityKind,
    name: str,
    version: str,
    columns="*",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DataFrame:
```

Read an entity from storage with optional column projection, partition-based
date filtering, and ML-friendly type coercion (Decimal→double, Long→int).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `kind` | `EntityKind` | Required | Entity type |
| `name` | `str` | Required | Entity name |
| `version` | `str` | Required | Entity version |
| `columns` | `List[str] \| "*"` | `"*"` | Columns to select (primary keys and partition columns always included) |
| `start_date` | `Optional[str]` | `None` | Inclusive partition filter (YYYY-MM-DD) |
| `end_date` | `Optional[str]` | `None` | Inclusive partition filter (YYYY-MM-DD) |

**Usage:**

```python
features = client.get_entity(
    EntityKind.FEATURE_VIEW, "user_features", "v1",
    columns=["age", "score"],
    start_date="2024-01-01", end_date="2024-01-31",
)
features.show()
```

---

#### `get_dataset`

```python
def get_dataset(
    self, name: str, version: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DataFrame:
```

Shorthand for `get_entity(EntityKind.DATASET, name, version, ...)`.

---

#### `get_training_set`

```python
def get_training_set(
    self, name: str, version: str,
    split: str = "train",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DataFrame:
```

Read a specific split. Appends `/{split}` to the storage base path.

**Usage:**

```python
train = client.get_training_set("churn_training", "v1", split="train")
val = client.get_training_set("churn_training", "v1", split="validation")
```

---

### Feature Assembly

#### `get_model_features`

```python
def get_model_features(
    self,
    model_name: str,
    model_version: str,
    query_df,
    start_date: str,
    end_date: Optional[str] = None,
    checkpoint_interval: int = 5,
    checkpoint_dir: Optional[str] = None,
    checkpoint_ctx: Optional[CheckpointContext] = None,
) -> DataFrame:
```

Assemble a wide feature DataFrame by joining all feature views declared in a
registered model config. Join keys are resolved automatically from the model's
`primary_keys` and `partition_columns`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_name` | `str` | Required | Registered model name |
| `model_version` | `str` | Required | Registered model version |
| `query_df` | `DataFrame` | Required | Base DataFrame with join keys |
| `start_date` | `str` | Required | Feature data start date (YYYY-MM-DD) |
| `end_date` | `Optional[str]` | `None` | Feature data end date (YYYY-MM-DD) |
| `checkpoint_interval` | `int` | `5` | Joins between Spark checkpoints |
| `checkpoint_dir` | `Optional[str]` | `None` | Directory for Spark checkpoints (legacy) |
| `checkpoint_ctx` | `Optional[CheckpointContext]` | `None` | Shared checkpoint context (preferred) |

**Checkpoint behavior:**
- When the number of feature-view joins exceeds `checkpoint_interval`, Spark
  `checkpoint(eager=True)` is called to break lineage.
- If `checkpoint_ctx` is provided, its path is used and cleanup is managed by
  the context manager. `checkpoint_dir` is ignored.
- If `checkpoint_dir` is provided without `checkpoint_ctx`, a UUID-based
  subdirectory is created and cleaned up after the operation.
- If neither is provided but checkpointing is needed, `ValueError` is raised.

**Usage — standalone:**

```python
query = spark.createDataFrame([(1, "2024-01-01")], ["user_id", "dt"])

features = client.get_model_features(
    "churn_model", "v1", query,
    start_date="2024-01-01",
    checkpoint_dir="/tmp/checkpoints",
)
```

**Usage — with checkpoint context:**

```python
with client.checkpoint_context("/tmp/ckpt") as ctx:
    features = client.get_model_features(
        "churn_model", "v1", query,
        start_date="2024-01-01",
        checkpoint_ctx=ctx,
    )
# checkpoint cleaned automatically
```

---

### Training Set Export

#### `export_training_dataset`

```python
def export_training_dataset(
    self,
    query_df,
    model_name: str,
    model_version: str,
    label_name: str,
    label_version: str,
    feature_start_date: str,
    feature_end_date: Optional[str] = None,
    lookback: Optional[Lookback] = None,
    join_type: str = "left",
    dry_run: bool = True,
    output_path: Optional[str] = None,
    checkpoint_ctx=None,
) -> DataFrame:
```

Point-in-time join of features (from a registered model) with labels. Applies
a lookback window to align label dates with feature dates for temporally
correct training data.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query_df` | `DataFrame` | Required | Base DataFrame with join keys |
| `model_name` | `str` | Required | Model name for feature assembly |
| `model_version` | `str` | Required | Model version |
| `label_name` | `str` | Required | Label entity name |
| `label_version` | `str` | Required | Label entity version |
| `feature_start_date` | `str` | Required | Feature data start (YYYY-MM-DD) |
| `feature_end_date` | `Optional[str]` | `None` | Feature data end (YYYY-MM-DD) |
| `lookback` | `Optional[Lookback]` | `Lookback("day", 1)` | Label time shift |
| `join_type` | `str` | `"left"` | Spark join type |
| `dry_run` | `bool` | `True` | If False and output_path set, materialize |
| `output_path` | `Optional[str]` | `None` | Output Parquet path |
| `checkpoint_ctx` | `Optional[CheckpointContext]` | `None` | Shared checkpoint context |

**Lookback behavior:**
- Label partition dates are shifted *back* by `periods` `freq` units
- For `freq="day"`: uses `date_sub` on the label partition column
- For `freq="month"`: uses `add_months` with negative offset
- Default: `Lookback(freq="day", periods=1)` — 1-day lookback

**Usage — dry run:**

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

**Usage — materialize with checkpoint context:**

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

#### `checkpoint_context`

```python
@contextmanager
def checkpoint_context(self, base_dir: str) -> CheckpointContext:
```

Context manager that creates an isolated UUID-based checkpoint directory,
configures Spark's checkpoint dir, and guarantees cleanup on exit.

| Parameter | Type | Description |
|-----------|------|-------------|
| `base_dir` | `str` | Parent directory for the checkpoint subdirectory |

Returns a `CheckpointContext` that can be passed to `get_model_features` and
`export_training_dataset`.

**Usage:**

```python
with client.checkpoint_context("/tmp/checkpoints") as ctx:
    features = client.get_model_features(..., checkpoint_ctx=ctx)
    dataset = client.export_training_dataset(..., checkpoint_ctx=ctx,
                                              dry_run=False, output_path="...")
# checkpoint cleaned here — after all Spark writes complete
```

---

### `CheckpointContext`

```python
class CheckpointContext:
    backend: StorageBackend
    path: str

    def cleanup(self): ...
```

Holds a checkpoint directory path. The owning context manager calls `cleanup`
on exit. Users typically don't instantiate this directly — use
`client.checkpoint_context()` instead.

---

### Management Operations

#### `list_entities`

```python
def list_entities(
    self, kind: Optional[EntityKind] = None
) -> List[Dict]:
```

List registered entities, optionally filtered by kind. Returns the parsed YAML
data dicts (which include `"name"`, `"version"`, `"kind"` keys).

**Usage:**

```python
fvs = client.list_entities(kind=EntityKind.FEATURE_VIEW)
for fv in fvs:
    print(fv["name"], fv["version"])
```

---

#### `get_entity_info`

```python
def get_entity_info(
    self, kind: EntityKind, name: str, version: str
) -> Config:
```

Return the typed config dataclass for a registered entity. Raises
`EntityNotFoundError` if not found.

**Usage:**

```python
config = client.get_entity_info(
    EntityKind.FEATURE_VIEW, "user_features", "v1"
)
print(config.primary_keys[0].name)  # "user_id"
print(config.storage.base_path)     # "gs://bucket/features/user_features/v1"
```

---

#### `sync_lifecycle`

```python
def sync_lifecycle(self) -> Dict[str, int]:
```

Scan all registry YAMLs, extract retention policies, and apply GCS lifecycle
rules. Only affects entities with GCS storage paths.

Returns `Dict[str, int]` mapping `"name/version"` → status (`1` = success, `-1` = failure).

**Usage:**

```python
result = client.sync_lifecycle()
# {"user_features/v1": 1, "churn_label/v1": 1}
```

---

#### `build_dependency_graph`

```python
def build_dependency_graph(
    self, name: str, version: str
) -> Dict[str, List[str]]:
```

Perform recursive BFS over entity dependencies. Searches all entity kinds to
find the starting entity, then follows `dependency` references.

Returns adjacency dict mapping `"name/version"` → `["dep1/v1", ...]`.

**Usage:**

```python
graph = client.build_dependency_graph("churn_model", "v1")
# {
#   "churn_model/v1": ["user_fv/v1"],
#   "user_fv/v1": ["raw_data/v1"],
#   "raw_data/v1": [],
# }
```

---

#### `migrate_registry`

```python
def migrate_registry(self, dry_run: bool = True) -> List[str]:
```

Scan all registry YAMLs for legacy format (missing `kind` key or other old
schema patterns) and convert to the current format.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dry_run` | `bool` | `True` | If True, preview only; if False, write back |

Returns list of paths that were (or would be) migrated.

**Usage:**

```python
# Preview changes
changes = client.migrate_registry(dry_run=True)
print(f"Files to migrate: {len(changes)}")

# Apply migration
migrated = client.migrate_registry(dry_run=False)
print(f"Migrated: {migrated}")
```

---

## Cross-References

- [Schema Module](api_schema.md) — config dataclasses used for registration
- [Storage Module](api_storage.md) — backend selection via `registry_dir`
- [Registry Module](api_registry.md) — internal functions delegated to by client
- [Types Module](api_types.md) — `EntityKind` enum used throughout
```

- [ ] **Step 2: Verify file**

```bash
wc -l docs/api_client.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/api_client.md
git commit -m "docs: add client module API reference"
```

---

### Task 9: Update README.md documentation links

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the Documentation links table in README.md**

Replace the existing Documentation table:

```markdown
## Documentation

| Document | Description |
|----------|-------------|
| [API Reference](docs/API.md) | Module overview and full client API reference |
| [CI/CD](docs/CI_CD.md) | Continuous integration and deployment pipeline |
| [Changelog](CHANGELOG.md) | Version history and release notes |
| [Design Specs](docs/superpowers/specs/) | Design documents for each feature |
```

with:

```markdown
## Documentation

| Document | Description |
|----------|-------------|
| [Project Overview](docs/overview.md) | Project purpose, structure, and capabilities |
| [API Index](docs/api_index.md) | Catalog of all APIs by module |
| [Types Module](docs/api_types.md) | Enums: EntityKind, StorageFormat, UpdateFrequency |
| [Schema Module](docs/api_schema.md) | Config dataclasses: FeatureViewConfig, ModelConfig, etc. |
| [Storage Module](docs/api_storage.md) | Backend abstraction: LocalBackend, GCSBackend |
| [Registry Module](docs/api_registry.md) | YAML serialization, validation, lifecycle |
| [Client Module](docs/api_client.md) | FeatureStoreClient — user-facing API |
| [CI/CD](docs/CI_CD.md) | Continuous integration and deployment pipeline |
| [Changelog](CHANGELOG.md) | Version history and release notes |
| [Design Specs](docs/superpowers/specs/) | Design documents for each feature |
```

- [ ] **Step 2: Verify update**

```bash
grep -c "docs/overview.md" README.md
```
Expected: `1`

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: update README links for new documentation structure"
```

---

### Task 10: Final verification

**Files:**
- Verify: all new docs exist, old API.md is gone

- [ ] **Step 1: Verify file structure**

```bash
echo "=== Expected files ===" && \
ls -la docs/overview.md docs/api_index.md docs/api_types.md docs/api_schema.md \
    docs/api_storage.md docs/api_registry.md docs/api_client.md && \
echo "=== API.md should NOT exist ===" && \
test -f docs/API.md && echo "ERROR: API.md still exists" || echo "OK: API.md removed"
```

Expected: All 7 files exist, confirmation that API.md is gone.

- [ ] **Step 2: Verify README link is updated**

```bash
grep "docs/overview.md" README.md
grep "docs/api_index.md" README.md
grep "docs/API.md" README.md
```

Expected: First two find matches, last one finds nothing (no reference to old API.md).

- [ ] **Step 3: Verify no broken internal links**

```bash
for f in docs/api_*.md docs/overview.md; do
    echo "=== $f ==="
    grep -oP '\[.*?\]\((.*?\.md)\)' "$f" | while read -r link; do
        target=$(echo "$link" | grep -oP '(?<=\().*?(?=\))')
        if [ ! -f "docs/$target" ]; then
            echo "  BROKEN: $target"
        fi
    done
done
```

Expected: No "BROKEN" output.

- [ ] **Step 4: Final commit (if any fixes needed)**

```bash
git status
```
