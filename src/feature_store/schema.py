"""Typed configuration dataclasses for the feature store."""

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ColumnSpec:
    """Describes a single column in a feature view or model schema."""
    name: str
    type: str
    is_label: bool = False
    feature_view: Optional[str] = None
    feature_view_version: Optional[str] = None


@dataclass
class KeySpec:
    """Describes a primary key or partition column."""
    name: str
    type: str
    format: Optional[str] = None


@dataclass
class Dependency:
    """A dependency on another entity (e.g. upstream feature view or model)."""
    name: str
    version: str


@dataclass
class PipelineSpec:
    """Pipeline execution parameters."""
    update_frequency: str = "daily"
    source_job: str = ""
    alert_threshold_hours: int = 48


@dataclass
class RetentionSpec:
    """Data retention settings."""
    ttl_days: int = 360
    cold_tier_days: int = 180


@dataclass
class StorageSpec:
    """Physical storage location for an entity."""
    base_path: str
    format: str = "parquet"


@dataclass
class Lookback:
    """Lookback window for feature computation."""
    freq: str = "day"
    periods: int = 1

    def __post_init__(self):
        if self.freq not in ("day", "month"):
            raise ValueError(
                f"freq must be 'day' or 'month', got {self.freq!r}"
            )


_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass
class _BaseConfig:
    """Base config with shared fields and name validation."""
    name: str
    version: str
    owner: str = ""
    description: str = ""
    dependency: List[Dependency] = field(default_factory=list)
    primary_keys: List[KeySpec] = field(default_factory=list)
    partition_columns: List[KeySpec] = field(default_factory=list)

    def __post_init__(self):
        if not _NAME_RE.match(self.name):
            raise ValueError(
                f"name must match pattern [a-z][a-z0-9_]*, got {self.name!r}"
            )

    def _check_no_overlap(self, schema: List[ColumnSpec]):
        pk_names = {k.name for k in self.primary_keys}
        pc_names = {k.name for k in self.partition_columns}
        schema_names = {c.name for c in schema}
        if pk_names & schema_names:
            raise ValueError(
                "primary key names and schema column names must not overlap"
            )
        if pc_names & schema_names:
            raise ValueError(
                "partition column names and schema column names must not overlap"
            )
        if pk_names & pc_names:
            raise ValueError(
                "primary key names and partition column names must not overlap"
            )


@dataclass
class FeatureViewConfig(_BaseConfig):
    """Configuration for a feature view entity."""
    storage: Optional[StorageSpec] = None
    pipeline: PipelineSpec = field(default_factory=PipelineSpec)
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "feature_view"

    def __post_init__(self):
        super().__post_init__()
        if not self.primary_keys:
            raise ValueError("primary_keys must not be empty")
        if not self.partition_columns:
            raise ValueError("partition_columns must not be empty")
        self._check_no_overlap(self.schema)


@dataclass
class ModelConfig(_BaseConfig):
    """Configuration for a model entity."""
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "model"

    def __post_init__(self):
        super().__post_init__()
        for col in self.schema:
            if col.feature_view is None:
                raise ValueError(
                    f"Model schema column {col.name!r} must have feature_view"
                )
            if col.feature_view_version is None:
                raise ValueError(
                    f"Model schema column {col.name!r} must have feature_view_version"
                )


@dataclass
class LabelConfig(_BaseConfig):
    """Configuration for a label entity."""
    storage: Optional[StorageSpec] = None
    schema: List[ColumnSpec] = field(default_factory=list)
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    kind: str = "label"


@dataclass
class DatasetConfig(_BaseConfig):
    """Configuration for a dataset entity."""
    storage: Optional[StorageSpec] = None
    kind: str = "dataset"


TrainingSetConfig = DatasetConfig
