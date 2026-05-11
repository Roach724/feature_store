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
