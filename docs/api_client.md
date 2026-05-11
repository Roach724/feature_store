# Module: `feature_store.client`

The primary user-facing API. `FeatureStoreClient` provides a unified interface
for registering, writing, reading, and assembling feature store entities.

**Source:** `src/feature_store/client.py`

---

## Public Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `FeatureStoreClient` | Class | Main entry point for all feature store operations |
| `CheckpointContext` | Class | Holds checkpoint directory path; used by `checkpoint_context` context manager |

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
