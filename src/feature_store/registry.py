"""Registry module for YAML serialization, validation, and lifecycle management."""

import logging
import os
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import yaml

from feature_store.types import EntityKind
from feature_store.schema import (
    ColumnSpec,
    KeySpec,
    Dependency,
    PipelineSpec,
    RetentionSpec,
    StorageSpec,
    FeatureViewConfig,
    ModelConfig,
    LabelConfig,
    DatasetConfig,
)
from feature_store.storage import LocalBackend

logger = logging.getLogger("feature_store")


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

class FeatureStoreError(Exception):
    """Base exception for all feature-store errors."""


class EntityNotFoundError(FeatureStoreError):
    """Requested entity does not exist in the registry."""


class ColumnNotFoundError(FeatureStoreError):
    """A requested column was not found."""


class SchemaValidationError(FeatureStoreError):
    """A schema validation failure."""


class MissingColumnsError(SchemaValidationError):
    """DataFrame is missing required columns."""


class ExtraColumnsError(SchemaValidationError):
    """DataFrame contains columns not declared in the config."""


class RegistryFormatError(FeatureStoreError):
    """Malformed or unsupported registry content."""


class StorageError(FeatureStoreError):
    """A storage-level operation failed."""


# ---------------------------------------------------------------------------
# Kind-to-class mapping
# ---------------------------------------------------------------------------

_KIND_TO_CLASS: Dict[str, type] = {
    "feature_view": FeatureViewConfig,
    "model": ModelConfig,
    "label": LabelConfig,
    "dataset": DatasetConfig,
    "training_set": DatasetConfig,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _drop_none(d: dict) -> dict:
    """Return *d* with any keys whose value is ``None`` removed."""
    return {k: v for k, v in d.items() if v is not None}


def _serialize_column(col: ColumnSpec) -> dict:
    """Serialize a single schema column, omitting falsy optional fields."""
    d: dict = {"name": col.name, "type": col.type}
    if col.is_label:
        d["is_label"] = col.is_label
    if col.feature_view:
        d["feature_view"] = col.feature_view
    if col.feature_view_version:
        d["feature_view_version"] = col.feature_view_version
    return d


# ---------------------------------------------------------------------------
# YAML serialization
# ---------------------------------------------------------------------------

def config_to_yaml(config) -> str:
    """Serialize any config dataclass to a YAML string."""
    data: dict = {}

    # Always-present identity fields
    data["kind"] = config.kind
    data["name"] = config.name
    data["version"] = config.version

    # Optional scalar fields -- only emit when truthy
    if config.owner:
        data["owner"] = config.owner
    if config.description:
        data["description"] = config.description

    # Lists -- always present (emit when non-empty)
    if config.dependency:
        data["dependency"] = [_drop_none(asdict(d)) for d in config.dependency]
    data["primary_keys"] = [_drop_none(asdict(k)) for k in config.primary_keys]
    data["partition_columns"] = [_drop_none(asdict(k)) for k in config.partition_columns]

    # Optional storage
    if getattr(config, "storage", None) is not None:
        data["storage"] = _drop_none(asdict(config.storage))

    # Pipeline (FeatureViewConfig only in practice)
    if hasattr(config, "pipeline"):
        data["pipeline"] = _drop_none(asdict(config.pipeline))  # type: ignore[union-attr]

    # Retention (FeatureViewConfig, LabelConfig)
    if hasattr(config, "retention"):
        data["retention"] = _drop_none(asdict(config.retention))  # type: ignore[union-attr]

    # Schema (FeatureViewConfig, ModelConfig, LabelConfig)
    if hasattr(config, "schema"):
        schema_data: List[ColumnSpec] = getattr(config, "schema")
        if schema_data:
            data["schema"] = [_serialize_column(c) for c in schema_data]

    return yaml.dump(
        data,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        indent=2,
    )


def yaml_to_config(data: Dict[str, Any]):
    """Deserialize a dict (loaded from YAML) back into a typed config object."""
    if "kind" not in data:
        raise ValueError("Missing 'kind' in YAML data")

    kind = data["kind"]
    cls = _KIND_TO_CLASS.get(kind)
    if cls is None:
        raise ValueError(f"Unknown kind: {kind!r}")

    # Collect only the kwargs that are present in *data* so we never pass
    # unexpected arguments to a particular config dataclass.
    kwargs: dict = {
        "name": data.get("name", ""),
        "version": data.get("version", "v1"),
    }

    if "owner" in data:
        kwargs["owner"] = data["owner"]
    if "description" in data:
        kwargs["description"] = data["description"]
    if "dependency" in data:
        kwargs["dependency"] = [Dependency(**d) for d in data["dependency"]]
    if "primary_keys" in data:
        kwargs["primary_keys"] = [KeySpec(**k) for k in data["primary_keys"]]
    if "partition_columns" in data:
        kwargs["partition_columns"] = [KeySpec(**k) for k in data["partition_columns"]]
    if "storage" in data:
        kwargs["storage"] = StorageSpec(**data["storage"])
    if "schema" in data:
        kwargs["schema"] = [ColumnSpec(**c) for c in data["schema"]]
    if "pipeline" in data:
        kwargs["pipeline"] = PipelineSpec(**data["pipeline"])
    if "retention" in data:
        kwargs["retention"] = RetentionSpec(**data["retention"])

    return cls(**kwargs)


def load_yaml(backend, path: str) -> Dict[str, Any]:
    """Load a YAML file from *backend* at *path* and return its parsed dict."""
    with backend.open(path, "r") as f:
        return yaml.safe_load(f)


def write_yaml(backend, path: str, config) -> str:
    """Serialize *config* to YAML and write it through *backend* at *path*.

    Returns *path* for chaining convenience.
    """
    yaml_str = config_to_yaml(config)
    # Ensure parent directories exist (only needed for LocalBackend)
    if isinstance(backend, LocalBackend):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
    with backend.open(path, "w") as f:
        f.write(yaml_str)
    return path


# ---------------------------------------------------------------------------
# Registry path helper
# ---------------------------------------------------------------------------

def _registry_path(registry_dir: str, kind: EntityKind, name: str, version: str) -> str:
    """Build the canonical path for a given entity YAML file."""
    return os.path.join(registry_dir, kind.value, f"{name}_{version}.yaml")


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def validate_dataframe(df, config, allow_extra_columns: bool = False) -> None:
    """Validate that *df* has the columns declared by *config*.

    Raises :class:`MissingColumnsError` if any expected column is missing.
    Raises :class:`ExtraColumnsError` if extra columns are present and
    *allow_extra_columns* is ``False``.
    """
    expected: set = set()
    for k in config.primary_keys:
        expected.add(k.name)
    for k in config.partition_columns:
        expected.add(k.name)
    for c in config.schema:
        expected.add(c.name)

    actual = set(df.columns)

    # Check extra columns first so that the test for extra-column detection
    # works even when partition columns happen to be absent from the test DF.
    extra = actual - expected
    if extra and not allow_extra_columns:
        raise ExtraColumnsError(
            f"unknown columns in {config.kind} {config.name!r}: {sorted(extra)!r}"
        )

    missing = expected - actual
    if missing:
        raise MissingColumnsError(
            f"missing columns in {config.kind} {config.name!r}: {sorted(missing)!r}"
        )


# ---------------------------------------------------------------------------
# Spark type helpers
# ---------------------------------------------------------------------------

def _spark_type_to_str(spark_type) -> str:
    """Map a PySpark ``DataType`` instance to a simple type name string."""
    from pyspark.sql.types import (  # type: ignore[import-untyped]
        StringType, IntegerType, LongType, DoubleType, FloatType,
        BooleanType, DateType, TimestampType, DecimalType,
    )

    mapping = {
        StringType: "string",
        IntegerType: "integer",
        LongType: "long",
        DoubleType: "double",
        FloatType: "float",
        BooleanType: "boolean",
        DateType: "date",
        TimestampType: "timestamp",
        DecimalType: "decimal",
    }
    for type_cls, name in mapping.items():
        if isinstance(spark_type, type_cls):
            return name
    return spark_type.simpleString()


# ---------------------------------------------------------------------------
# Build config from DataFrame
# ---------------------------------------------------------------------------

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
):
    """Infer a typed config from a Spark DataFrame schema.

    Columns listed in *primary_keys* or *partition_columns* are excluded from
    the inferred schema.
    """
    pk_set = set(primary_keys)
    pc_set = set(partition_columns)

    schema: list = []
    for field in df.schema.fields:
        if field.name in pk_set or field.name in pc_set:
            continue
        schema.append(ColumnSpec(
            name=field.name,
            type=_spark_type_to_str(field.dataType),
        ))

    key_specs = [KeySpec(name=k, type="string") for k in primary_keys]
    pc_specs = [KeySpec(name=k, type="string") for k in partition_columns]
    storage = StorageSpec(base_path=storage_base_path)

    if kind == EntityKind.FEATURE_VIEW:
        return FeatureViewConfig(
            name=name, version=version,
            owner=owner, description=description,
            primary_keys=key_specs,
            partition_columns=pc_specs,
            storage=storage,
            schema=schema,
        )
    elif kind == EntityKind.MODEL:
        return ModelConfig(
            name=name, version=version,
            owner=owner, description=description,
            primary_keys=key_specs,
            partition_columns=pc_specs,
            schema=schema,
        )
    elif kind == EntityKind.LABEL:
        return LabelConfig(
            name=name, version=version,
            owner=owner, description=description,
            primary_keys=key_specs,
            partition_columns=pc_specs,
            storage=storage,
            schema=schema,
        )
    elif kind in (EntityKind.DATASET, EntityKind.TRAINING_SET):
        return DatasetConfig(
            name=name, version=version,
            owner=owner, description=description,
            primary_keys=key_specs,
            partition_columns=pc_specs,
            storage=storage,
        )
    else:
        raise ValueError(f"Unsupported entity kind: {kind!r}")


# ---------------------------------------------------------------------------
# Build model config from feature references
# ---------------------------------------------------------------------------

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
    """Build a :class:`ModelConfig` from a list of feature references.

    *features* may be a list of dicts (matching :class:`ColumnSpec` fields) or
    legacy URN strings of the form ``feature_view:name/version/column``.
    """
    schema: list = []
    for feat in features:
        if isinstance(feat, dict):
            schema.append(ColumnSpec(**feat))
        elif isinstance(feat, str):
            # Legacy URN: "feature_view:fv_name/v1/col_name"
            # Strip the "feature_view:" prefix if present.
            rest = feat.split(":", 1)[-1] if feat.startswith("feature_view:") else feat
            parts = rest.split("/")
            if len(parts) >= 3:
                schema.append(ColumnSpec(
                    name=parts[-1],
                    type="string",
                    feature_view=parts[0],
                    feature_view_version=parts[1],
                ))
            else:
                raise ValueError(
                    f"Cannot parse feature URN {feat!r}: "
                    "expected 'feature_view:name/version/column'"
                )
        else:
            raise ValueError(
                f"Unsupported feature reference type {type(feat).__name__}: {feat!r}"
            )

    key_specs = [KeySpec(name=k, type="string") for k in primary_keys]
    pc_specs = [KeySpec(name=k, type="string") for k in partition_columns]

    return ModelConfig(
        name=name, version=version,
        owner=owner, description=description,
        primary_keys=key_specs,
        partition_columns=pc_specs,
        schema=schema,
        dependency=dependency or [],
    )


# ---------------------------------------------------------------------------
# List entities
# ---------------------------------------------------------------------------

def list_entities(
    backend,
    registry_dir: str,
    kind: Optional[EntityKind] = None,
) -> List[Dict]:
    """Scan *registry_dir* for entity YAML files and return their contents.

    Each entry is a dict with keys ``"path"`` and ``"data"``.
    """
    pattern = os.path.join(registry_dir, "**", "*.yaml")
    entities: List[Dict] = []
    for path in backend.glob(pattern):
        data = load_yaml(backend, path)
        if kind is None or data.get("kind") == kind.value:
            entities.append({"path": path, "data": data})
    return entities


# ---------------------------------------------------------------------------
# Lifecycle sync
# ---------------------------------------------------------------------------

def _apply_gcs_lifecycle(bucket_name: str, prefix: str, ttl_days: int) -> None:
    """Create or update a GCS lifecycle rule for a prefix and TTL."""
    try:
        from google.cloud import storage  # type: ignore[import-untyped]
    except ImportError:
        logger.warning(
            "google-cloud-storage is not installed; skipping lifecycle sync"
        )
        return

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    # Build a new lifecycle rule for this prefix.
    rule = {
        "action": {"type": "Delete"},
        "condition": {"age": ttl_days, "matchesPrefix": [prefix]},
    }

    # Merge with existing rules, replacing any matching the same prefix.
    rules = list(bucket.lifecycle_rules) if bucket.lifecycle_rules else []
    rules = [
        r for r in rules
        if not (
            isinstance(r, dict)
            and r.get("condition", {}).get("matchesPrefix") == [prefix]
        )
    ]
    rules.append(rule)
    bucket.lifecycle_rules = rules
    bucket.update()
    logger.info(
        "Applied lifecycle rule: delete after %d days for gs://%s/%s",
        ttl_days, bucket_name, prefix,
    )


def sync_lifecycle(backend, registry_dir: str) -> Dict[str, int]:
    """Scan registry YAMLs for retention configs and apply GCS lifecycle rules.

    Returns a dict mapping ``"name/version"`` to status codes:
    ``1`` for success, ``-1`` for failure.
    """
    status: Dict[str, int] = {}

    for entity in list_entities(backend, registry_dir):
        data = entity["data"]

        # Only entities with storage + retention need lifecycle management.
        retention = data.get("retention")
        if retention is None:
            continue

        storage = data.get("storage")
        if storage is None:
            continue

        base_path = storage.get("base_path", "")
        if not base_path.startswith("gs://"):
            continue  # lifecycle only applies to GCS

        ttl_days = retention.get("ttl_days", 360)
        key = f"{data['name']}/{data['version']}"

        # Extract bucket and prefix from gs://bucket/prefix/...
        parts = base_path[5:].split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        try:
            _apply_gcs_lifecycle(bucket_name, prefix, ttl_days)
            status[key] = 1
        except Exception as exc:
            logger.warning(
                "Failed to sync lifecycle for %s: %s", key, exc,
            )
            status[key] = -1

    return status
