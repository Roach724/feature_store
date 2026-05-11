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
