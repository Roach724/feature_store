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
        ColumnSpec(name="age", type="integer",
                   feature_view="user_fv", feature_view_version="v1"),
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
| `columns` | `List[str] \| "*"` | `"*"` | Columns to read |
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
| `query_df` | `DataFrame` | — | Query DataFrame with primary keys + partition column |
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

Point-in-time join of features and labels with configurable lookback.

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
| `dry_run` | `bool` | `True` | If False + output_path set, materialize |
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
    dataset = client.export_training_dataset(..., checkpoint_ctx=ctx,
                                              dry_run=False, output_path="...")
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
