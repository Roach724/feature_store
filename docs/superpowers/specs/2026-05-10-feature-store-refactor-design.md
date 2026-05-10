# Feature Store Client Refactor — Design Spec

Date: 2026-05-10
Status: approved

## Overview

Refactor the Feature Store client project: flat `src/` structure, unified YAML format, integrated registration API, pluggable storage backends.

---

## 1. Project Structure

```
feature_store/
├── src/
│   ├── __init__.py          # Public API: exports FeatureStoreClient
│   ├── client.py            # Main client — orchestration, user-facing methods
│   ├── registry.py          # YAML read/write/validate, lifecycle sync, migration
│   ├── storage.py           # Storage backends (GCS, S3, local)
│   ├── schema.py            # Dataclasses for typed config objects
│   └── types.py             # Enums & constants
├── tests/
│   ├── conftest.py
│   ├── test_schema.py
│   ├── test_registry.py
│   ├── test_storage.py
│   ├── test_client.py
│   ├── test_migration.py
│   └── fixtures/
│       ├── old_format/
│       └── new_format/
├── docs/
│   ├── superpowers/
│   │   └── specs/
│   └── examples/            # Sample YAML files (migrated to new format)
└── pyproject.toml
```

Remove: `core/`, `scripts/`, `sync_project_resources.sh` (replaced by package build).

---

## 2. Module Boundaries

### `types.py` — Zero-dependency enums and constants

```python
class EntityKind(enum.Enum):
    FEATURE_VIEW = "feature_view"
    MODEL = "model"
    LABEL = "label"
    DATASET = "dataset"
    TRAINING_SET = "training_set"

class StorageFormat(enum.Enum):
    PARQUET = "parquet"

class UpdateFrequency(enum.Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
```

### `schema.py` — Config dataclasses for config-first registration

Each config class has a `kind` class-attribute matching EntityKind. All use Python dataclasses with `__post_init__` validation.

```python
@dataclass
class ColumnSpec:
    name: str
    type: str               # "string" | "integer" | "double" | "boolean"
    is_label: bool = False   # label entities only
    # model entities only
    feature_view: Optional[str] = None
    feature_view_version: Optional[str] = None

@dataclass
class KeySpec:
    name: str
    type: str
    format: Optional[str] = None  # e.g. "yyyy-MM", "yyyy-MM-dd"

@dataclass
class Dependency:
    name: str
    version: str

@dataclass
class PipelineSpec:
    update_frequency: str = "daily"
    source_job: str = ""
    alert_threshold_hours: int = 48

@dataclass
class RetentionSpec:
    ttl_days: int = 360
    cold_tier_days: int = 180

@dataclass
class StorageSpec:
    base_path: str
    format: str = "parquet"

@dataclass
class FeatureViewConfig:
    name: str
    version: str
    owner: str = ""
    description: str = ""
    dependency: List[Dependency] = field(default_factory=list)
    primary_keys: List[KeySpec] = field(default_factory=list)
    partition_columns: List[KeySpec] = field(default_factory=list)
    storage: Optional[StorageSpec] = None
    pipeline: PipelineSpec = field(default_factory=PipelineSpec)
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "feature_view"

@dataclass
class ModelConfig:
    name: str
    version: str
    owner: str = ""
    description: str = ""
    dependency: List[Dependency] = field(default_factory=list)
    primary_keys: List[KeySpec] = field(default_factory=list)
    partition_columns: List[KeySpec] = field(default_factory=list)
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "model"

@dataclass
class LabelConfig:
    name: str
    version: str
    owner: str = ""
    description: str = ""
    dependency: List[Dependency] = field(default_factory=list)
    primary_keys: List[KeySpec] = field(default_factory=list)
    partition_columns: List[KeySpec] = field(default_factory=list)
    storage: Optional[StorageSpec] = None
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "label"

@dataclass
class DatasetConfig:
    name: str
    version: str
    owner: str = ""
    description: str = ""
    dependency: List[Dependency] = field(default_factory=list)
    primary_keys: List[KeySpec] = field(default_factory=list)
    partition_columns: List[KeySpec] = field(default_factory=list)
    storage: Optional[StorageSpec] = None
    kind: str = "dataset"

TrainingSetConfig = DatasetConfig  # same shape, kind="training_set"
```

### `storage.py` — Pluggable backends

```python
class StorageBackend(ABC):
    def read_parquet(self, spark, path, columns=None, filters=None) -> DataFrame: ...
    def write_parquet(self, df, path, partition_cols, mode, compression="snappy"): ...
    def open(self, path, mode="r") -> IO: ...           # for YAML
    def exists(self, path) -> bool: ...
    def glob(self, pattern) -> List[str]: ...
    def cp(self, src, dst): ...
    def rm(self, path, recursive=False): ...

class GCSBackend(StorageBackend): ...
class LocalBackend(StorageBackend): ...

def get_backend(path: str) -> StorageBackend:
    """Factory: gs:// -> GCSBackend, /local/ -> LocalBackend, s3:// -> S3Backend"""
```

### `registry.py` — Registry operations

Core functions:
- `load_yaml(backend, path) -> Dict` — load from storage
- `yaml_to_config(data: Dict) -> Config` — parse + validate YAML into typed config
- `config_to_yaml(config: Config) -> str` — serialize config to YAML string
- `write_yaml(backend, path, config)` — write config to storage
- `list_entities(backend, registry_dir, kind=None) -> List[EntitySummary]` — scan registry
- `validate_dataframe(df, config) -> None` — schema column alignment check
- `build_config_from_df(df, kind, name, version, primary_keys, partition_columns, **kwargs) -> Config` — DataFrame schema inference
- `build_model_config(feature_urns_or_specs, name, version, registry_dir, **kwargs) -> ModelConfig` — build model from feature references
- `sync_lifecycle(backend, registry_dir) -> Dict[str, int]` — extract TTL rules, push to buckets
- `migrate_old_format(backend, registry_dir, dry_run=True) -> List[str]` — batch old→new format migration

### `client.py` — Main client class

Complete methods:

```python
class FeatureStoreClient:
    def __init__(self, spark: SparkSession, registry_dir: str): ...

    # Registration
    def register(self, source, kind=None, name=None, version="v1",
                 primary_keys=None, partition_columns=None, **kwargs) -> str: ...
    def register_feature_view(self, source, **kwargs) -> str: ...
    def register_model(self, source, **kwargs) -> str: ...
    def register_label(self, source, **kwargs) -> str: ...
    def register_dataset(self, source, **kwargs) -> str: ...
    def register_training_set(self, source, **kwargs) -> str: ...

    # Write (schema-validated, all entities must be pre-registered)
    def write_entity(self, df, kind, name, version, partition_num=24) -> None: ...

    # Read
    def get_entity(self, kind, name, version, columns="*",
                   start_date=None, end_date=None) -> DataFrame: ...
    def get_model_features(self, model_name, model_version, query_df,
                           start_date, end_date=None) -> DataFrame: ...

    # Dataset / Training Set
    def export_training_dataset(self, query_df, model_name, model_version,
                                label_name, label_version, feature_start_date,
                                feature_end_date=None, lookback=None,
                                join_type="left", dry_run=True,
                                output_path=None) -> DataFrame: ...
    def write_dataset(self, dataset, name, version, **kwargs) -> None: ...
    def get_dataset(self, name, version, start_date=None, end_date=None) -> DataFrame: ...
    def write_training_set(self, dataset, name, version, split="train") -> None: ...
    def get_training_set(self, name, version, split="train",
                         start_date=None, end_date=None) -> DataFrame: ...

    # Management
    def list_entities(self, kind=None) -> List[EntitySummary]: ...
    def get_entity_info(self, kind, name, version) -> Config: ...
    def sync_lifecycle(self) -> Dict[str, int]: ...
    def build_dependency_graph(self, name, version) -> DiGraph: ...
    def migrate_registry(self, dry_run=True) -> List[str]: ...
```

---

## 3. Unified YAML Format

### Feature View

```yaml
kind: feature_view
name: "mobile_app_features"
version: "v3"
owner: "derick_x_dang@pccw.com"
description: "Mobile app usage features per customer per month"

dependency:
  - name: "some_upstream_feature_view"
    version: "v1"

primary_keys:
  - name: "hashed_msisdn"
    type: "string"

partition_columns:
  - name: "feature_month"
    type: "string"
    format: "yyyy-MM"

storage:
  base_path: "gs://bucket/feature_store/feature_views/mobile_app_features/v3"
  format: "parquet"

pipeline:
  update_frequency: "monthly"
  source_job: "some_job_id"
  alert_threshold_hours: 26

retention:
  ttl_days: 360
  cold_tier_days: 180

schema:
  - name: "risky_apps_distinct_category"
    type: "integer"
  - name: "risky_apps_total_activity_days"
    type: "double"
```

### Model Feature

```yaml
kind: model
name: "pcd_acq_model"
version: "v4"
owner: "derick_x_dang@pccw.com"
description: "Acquisition model features"

dependency:
  - name: "base_user_model"
    version: "v2"

primary_keys:
  - name: "hashed_msisdn"
    type: "string"

partition_columns:
  - name: "feature_month"
    type: "string"
    format: "yyyy-MM"

schema:
  - name: "club_orig_status"
    type: "integer"
    feature_view: "club_features"
    feature_view_version: "v1"
  - name: "28Hse_30d_app_decay_ctime_count"
    type: "double"
    feature_view: "mobile_app_features"
    feature_view_version: "v3"
```

### Label

```yaml
kind: label
name: "pcd_acq_label"
version: "v1"
owner: "derick_x_dang@pccw.com"
description: "PCD acquisition label"

primary_keys:
  - name: "id_doc_num"
    type: "string"
  - name: "id_doc_type"
    type: "string"

partition_columns:
  - name: "label_month"
    type: "string"
    format: "yyyy-MM"

storage:
  base_path: "gs://bucket/feature_store/labels/pcd_acq_labels/v1"
  format: "parquet"

retention:
  ttl_days: 360
  cold_tier_days: 180

schema:
  - name: "acq_pcd"
    type: "integer"
    is_label: true
```

### Dataset / Training Set

```yaml
kind: dataset
name: "pcd_acq_training"
version: "v1"
owner: "derick_x_dang@pccw.com"
description: "Training data for pcd acq model"

dependency:
  - name: "pcd_acq_model"
    version: "v4"
  - name: "pcd_acq_label"
    version: "v1"

primary_keys:
  - name: "id_doc_num"
    type: "string"

partition_columns:
  - name: "feature_month"
    type: "string"
    format: "yyyy-MM"

storage:
  base_path: "gs://bucket/feature_store/datasets/pcd_acq_training/v1"
  format: "parquet"
```

### Key format changes from old

| Old | New | Reason |
|-----|-----|--------|
| `entity: "customers"` | Removed | Domain entity concept not needed; use tags if required |
| `entity_type` inferred from path | `kind` top-level field | Explicit, uniform |
| Model had no `version` | Model requires `version` | Consistency across all entities |
| `primary_keys` / partition col embedded in schema | Top-level `primary_keys` + `partition_columns` | schema contains only pure data columns |
| `is_primary_key: true/false` in schema | Moved to `primary_keys` | Clearer role separation |
| Partition column name in `storage.partition_column` | `partition_columns` with name/type/format | Multi-partition support, format metadata |
| Model `features` as URN list `"view@v1:feat"` | Structured `schema` with `feature_view` + `feature_view_version` | Type-safe, machine-readable, consistent with feature_view |
| No dependency tracking | Optional `dependency` on all entities | Enables DAG computation |
| No `format` on partition | Optional `format` field on partition columns | Supports date format-aware lookback |

---

## 4. Error Handling

### Exception hierarchy

```python
class FeatureStoreError(Exception): ...
class EntityNotFoundError(FeatureStoreError): ...
class ColumnNotFoundError(FeatureStoreError): ...
class SchemaValidationError(FeatureStoreError): ...
class MissingColumnsError(SchemaValidationError): ...
class ExtraColumnsError(SchemaValidationError): ...
class RegistryFormatError(FeatureStoreError): ...
class StorageError(FeatureStoreError): ...
```

### Validation points

| Phase | Check | Error |
|-------|-------|-------|
| Config construction | `name` matches `[a-z][a-z0-9_]*` | ValueError |
| Config construction | `primary_keys` / `partition_columns` non-empty | ValueError |
| Config construction | schema column names don't overlap with keys/partitions | ValueError |
| Config construction | model schema columns have `feature_view` + `feature_view_version` | ValueError |
| Register | `dependency` references exist (unless `validate_dependencies=False`) | EntityNotFoundError |
| Write | DataFrame columns match config schema | MissingColumnsError / ExtraColumnsError |
| Read | Requested column exists in schema | ColumnNotFoundError |
| Read | Requested entity exists in registry | EntityNotFoundError |

### Logging

Replace all `print()` with `logging.getLogger("feature_store")`. INFO for key milestones, DEBUG for schema details.

---

## 5. Testing

### Test coverage

```
tests/
├── conftest.py              # Spark session (local[2]), temp paths, sample configs
├── test_schema.py           # Config construction, validation, edge cases
├── test_registry.py         # YAML round-trip, build from df, lifecycle extraction
├── test_storage.py          # LocalBackend read/write/glob, GCSBackend (integration)
├── test_client.py           # End-to-end: register → write → read → model assembly
├── test_migration.py        # Old fixtures → new format, diff validation
└── fixtures/
    ├── old_format/
    │   ├── feature_view.yaml
    │   ├── model_feature.yaml
    │   └── label.yaml
    └── new_format/
        ├── feature_view.yaml
        ├── model_feature.yaml
        └── label.yaml
```

### Testing principles

- Unit tests (`test_schema`, `test_registry`, `test_storage`): no Spark dependency, use temporary files
- Integration tests (`test_client`): local Spark (master="local[2]") + LocalBackend
- Migration tests (`test_migration`): feed old format fixtures, assert output matches new format fixtures
- Don't mock storage — use LocalBackend for deterministic, fast tests

---

## 6. Migration

### `migrate_registry(dry_run=True) -> List[str]`

1. Scan registry directory for all `.yaml` files
2. Detect old format: missing `kind` field, or model `features` as URN list
3. Map old → new:
   - `entity_type` from path convention or `entity` field → `kind`
   - Extract `is_primary_key: true` columns from schema → `primary_keys`
   - Extract partition column from `storage.partition_column` → `partition_columns` (infer `format` from parquet data if possible, else leave blank)
   - Model URN list `"view@v1:feat"` → schema with `feature_view` + `feature_view_version`
   - Model without version → default `version: "v1"`
   - Discard `entity` field (domain entity name)
4. Write new format (or dry_run: report only)
5. Return list of changed file paths

### Migration type inference for model features

Model YAML with URN lists lacks type information. During migration, for each feature reference `view@version:feature_name`, resolve by loading the referenced feature view YAML and extracting the column type. If the feature view is unavailable, warn and default to `type: "string"`.

---

## 7. `export_training_dataset` — Structured Lookback

Replace scattered `lookback_freq` / `lookback_periods` parameters with:

```python
@dataclass
class Lookback:
    freq: Literal["day", "month"] = "day"
    periods: int = 1
```

Usage: `client.export_training_dataset(..., lookback=Lookback(freq="month", periods=1))`

Fix: month-level lookback previously used `%Y-%m` format strings which break date comparisons. Use proper `add_months` with full dates internally.

---

## 8. Spark Lineage Checkpoint — Model Feature Assembly

When assembling model features via `get_model_features`, multiple successive left-joins can produce deep Spark query plans (DAG blowup). The old code exposed this to the caller, who had to configure checkpoint directories externally.

### Design

The checkpoint mechanism is fully managed internally. Callers only need to provide a base checkpoint directory.

```python
def get_model_features(
    self,
    model_name: str,
    model_version: str,
    query_df: DataFrame,
    start_date: str,
    end_date: str = None,
    checkpoint_interval: int = 5,        # joins between checkpoints
    checkpoint_dir: str = None            # auto-generated if None
) -> DataFrame:
```

### Behavior

1. Before assembly starts, calculate total join count from model schema (number of distinct `(feature_view, feature_view_version)` pairs)
2. If total joins <= `checkpoint_interval`, skip checkpoint entirely
3. If checkpoint is needed, generate an isolated checkpoint path:
   - Path: `{checkpoint_dir}/{model_name}_{model_version}_{uuid4().hex[:8]}/`
   - Isolation ensures parallel writes to the same model (or different models) never conflict
4. After every `checkpoint_interval` joins, trigger `df.checkpoint(eager=True)` to cut lineage
5. **Cleanup:** After assembly completes (success or failure), delete the checkpoint directory

### Implementation outline

```python
import uuid
import logging

logger = logging.getLogger("feature_store")

def _get_model_features_inner(self, model_name, model_version, query_df,
                               start_date, end_date,
                               checkpoint_interval, checkpoint_dir):
    config = self._load_config("model", model_name, model_version)

    # Determine unique feature views to join
    view_versions = set()
    for col in config.schema:
        view_versions.add((col.feature_view, col.feature_view_version))

    total_joins = len(view_versions)
    needs_checkpoint = total_joins > checkpoint_interval

    if needs_checkpoint and not checkpoint_dir:
        raise ValueError("checkpoint_dir is required when model has > "
                         f"{checkpoint_interval} feature views ({total_joins})")

    checkpoint_path = None
    if needs_checkpoint:
        checkpoint_path = (f"{checkpoint_dir}/{model_name}_{model_version}_"
                           f"{uuid.uuid4().hex[:8]}")
        self.spark.sparkContext.setCheckpointDir(checkpoint_path)
        logger.info(f"Checkpoint dir: {checkpoint_path} "
                    f"(joins: {total_joins}, interval: {checkpoint_interval})")

    try:
        result_df = query_df
        join_count = 0

        for (view_name, view_version), feature_names in view_version_to_features.items():
            view_df = self.get_entity(
                kind=EntityKind.FEATURE_VIEW,
                name=view_name, version=view_version,
                columns=feature_names,
                start_date=start_date, end_date=end_date
            )
            # ... join logic ...

            join_count += 1
            if needs_checkpoint and join_count % checkpoint_interval == 0:
                logger.debug(f"Checkpoint at join {join_count}/{total_joins}")
                result_df = result_df.checkpoint(eager=True)

        return result_df
    finally:
        # Always cleanup checkpoint data to avoid accumulation
        if checkpoint_path and needs_checkpoint:
            self._backend.rm(checkpoint_path, recursive=True)
            logger.debug(f"Checkpoint cleaned: {checkpoint_path}")
```

### Key design decisions

| Decision | Rationale |
|----------|-----------|
| UUID-based subdirectory per call | Isolates parallel jobs; avoids checkpoint path conflicts |
| `finally` block cleanup | Guarantees cleanup even on exceptions |
| Pre-compute join count | Avoid checkpoint overhead for small models |
| `checkpoint_dir` required when needed | Fails fast with clear error instead of silently working with external config |
| Eager checkpoint | Matches existing behavior; non-eager could be added as option later |

### Spark configuration

The caller is responsible for Spark's `spark.sql.adaptive.enabled` and `spark.sql.adaptive.coalescePartitions.enabled` (typically enabled by default). These are NOT managed by the client — they belong to the Spark session configuration.
