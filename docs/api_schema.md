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
| `name` | `str` | Required | Entity name. Must match `^[a-z][a-z0-9_]*$` |
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
