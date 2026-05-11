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
