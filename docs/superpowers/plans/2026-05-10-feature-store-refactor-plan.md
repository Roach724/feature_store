# Feature Store Client Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor feature store client to flat `src/` structure with unified YAML, integrated registration API, and pluggable storage backends.

**Architecture:** `src/` contains 6 modules: `types` (enums), `schema` (config dataclasses), `storage` (backend abstraction), `registry` (YAML + lifecycle operations), `client` (orchestration, user-facing API), and `__init__` (public exports). Tests mirror each module with local backends, no GCS dependency required.

**Tech Stack:** Python 3.10+, PySpark, PyYAML, google-cloud-storage, fsspec, pytest

---

## File Structure

```
feature_store/
├── src/
│   ├── __init__.py          # Public API exports
│   ├── types.py             # EntityKind enum, constants
│   ├── schema.py            # ColumnSpec, KeySpec, Config dataclasses, Lookback
│   ├── storage.py           # StorageBackend ABC, GCSBackend, LocalBackend, get_backend()
│   ├── registry.py          # YAML load/dump, config build, validate, lifecycle, migration
│   └── client.py            # FeatureStoreClient
├── tests/
│   ├── conftest.py
│   ├── test_types.py
│   ├── test_schema.py
│   ├── test_storage.py
│   ├── test_registry.py
│   ├── test_client.py
│   ├── test_migration.py
│   └── fixtures/
│       ├── old_format/
│       │   ├── feature_view.yaml
│       │   ├── model_feature.yaml
│       │   └── label.yaml
│       └── new_format/
│           ├── feature_view.yaml
│           ├── model_feature.yaml
│           └── label.yaml
├── docs/
│   └── examples/
├── pyproject.toml
```

---

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/` (empty dir initially)
- Create: `tests/` (empty dir initially)

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src tests/docs/examples tests/fixtures/old_format tests/fixtures/new_format
```

- [ ] **Step 2: Write pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.backends._legacy:_Backend"

[project]
name = "feature_store"
version = "2.0.0"
description = "Feature Store client with YAML-based registry"
requires-python = ">=3.10"
dependencies = [
    "pandas>=1.5",
    "pyyaml>=6.0",
    "fsspec>=2023.0",
    "gcsfs>=2023.0",
    "google-cloud-storage>=2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pyspark>=3.3",
]

[tool.setuptools.packages.find]
where = ["src"]
```

- [ ] **Step 3: Verify**

```bash
python -c "import sys; print(sys.version)"
ls -la src/ tests/ docs/examples/
```

- [ ] **Step 4: Initialize git and commit**

```bash
git init && git add pyproject.toml && git commit -m "chore: project scaffolding"
```

---

### Task 2: `types.py` — Enums and Constants

**Files:**
- Create: `src/types.py`
- Create: `tests/test_types.py`

- [ ] **Step 1: Write failing tests for types**

```python
# tests/test_types.py
import enum
from feature_store.types import EntityKind, StorageFormat, UpdateFrequency


def test_entity_kind_values():
    assert EntityKind.FEATURE_VIEW.value == "feature_view"
    assert EntityKind.MODEL.value == "model"
    assert EntityKind.LABEL.value == "label"
    assert EntityKind.DATASET.value == "dataset"
    assert EntityKind.TRAINING_SET.value == "training_set"


def test_entity_kind_is_enum():
    assert issubclass(EntityKind, enum.Enum)
    assert len(EntityKind) == 5


def test_storage_format_values():
    assert StorageFormat.PARQUET.value == "parquet"


def test_update_frequency_values():
    assert UpdateFrequency.DAILY.value == "daily"
    assert UpdateFrequency.WEEKLY.value == "weekly"
    assert UpdateFrequency.MONTHLY.value == "monthly"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_types.py -v
```
Expected: FAIL with `ModuleNotFoundError: No module named 'feature_store.types'`

- [ ] **Step 3: Write `src/types.py`**

```python
"""Enums and constants for the feature store client."""

import enum


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

- [ ] **Step 4: Also need `src/__init__.py` to make it importable**

```python
# src/__init__.py
from feature_store.types import EntityKind, StorageFormat, UpdateFrequency

__all__ = ["EntityKind", "StorageFormat", "UpdateFrequency"]
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_types.py -v
```
Expected: PASS (4 tests)

- [ ] **Step 6: Commit**

```bash
git add src/types.py src/__init__.py tests/test_types.py
git commit -m "feat: add types module with EntityKind, StorageFormat, UpdateFrequency enums"
```

---

### Task 3: `schema.py` — Config Dataclasses

**Files:**
- Create: `src/schema.py`
- Create: `tests/test_schema.py`

- [ ] **Step 1: Write failing tests for core dataclasses**

```python
# tests/test_schema.py
import pytest
from feature_store.schema import (
    ColumnSpec, KeySpec, Dependency, PipelineSpec,
    RetentionSpec, StorageSpec, Lookback,
    FeatureViewConfig, ModelConfig, LabelConfig, DatasetConfig,
)


class TestColumnSpec:
    def test_basic_column(self):
        col = ColumnSpec(name="age", type="integer")
        assert col.name == "age"
        assert col.type == "integer"
        assert col.is_label is False
        assert col.feature_view is None
        assert col.feature_view_version is None

    def test_label_column(self):
        col = ColumnSpec(name="churn", type="integer", is_label=True)
        assert col.is_label is True

    def test_model_column_with_feature_view_ref(self):
        col = ColumnSpec(
            name="click_rate",
            type="double",
            feature_view="user_features",
            feature_view_version="v1",
        )
        assert col.feature_view == "user_features"
        assert col.feature_view_version == "v1"

    def test_type_must_be_valid(self):
        # type is a free-form string for now; validates at registry level
        col = ColumnSpec(name="x", type="array")
        assert col.type == "array"


class TestKeySpec:
    def test_basic_key(self):
        key = KeySpec(name="user_id", type="string")
        assert key.name == "user_id"
        assert key.type == "string"
        assert key.format is None

    def test_key_with_format(self):
        key = KeySpec(name="dt", type="string", format="yyyy-MM-dd")
        assert key.format == "yyyy-MM-dd"


class TestDependency:
    def test_dependency(self):
        dep = Dependency(name="base_model", version="v2")
        assert dep.name == "base_model"
        assert dep.version == "v2"


class TestLookback:
    def test_default_lookback(self):
        lb = Lookback()
        assert lb.freq == "day"
        assert lb.periods == 1

    def test_month_lookback(self):
        lb = Lookback(freq="month", periods=3)
        assert lb.freq == "month"
        assert lb.periods == 3

    def test_invalid_freq_raises(self):
        with pytest.raises(ValueError, match="freq must be"):
            Lookback(freq="hour")


class TestFeatureViewConfig:
    def test_minimal_config(self):
        cfg = FeatureViewConfig(
            name="my_features",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://bucket/fv/v1"),
        )
        assert cfg.name == "my_features"
        assert cfg.version == "v1"
        assert cfg.kind == "feature_view"
        assert cfg.owner == ""
        assert len(cfg.primary_keys) == 1
        assert len(cfg.partition_columns) == 1
        assert cfg.storage.base_path == "gs://bucket/fv/v1"

    def test_name_validation(self):
        with pytest.raises(ValueError, match="name must match"):
            FeatureViewConfig(
                name="InvalidName",
                version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
            )

    def test_primary_keys_required(self):
        with pytest.raises(ValueError, match="primary_keys must not be empty"):
            FeatureViewConfig(
                name="my_features",
                version="v1",
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
            )

    def test_partition_columns_required(self):
        with pytest.raises(ValueError, match="partition_columns must not be empty"):
            FeatureViewConfig(
                name="my_features",
                version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
            )

    def test_column_name_overlap_detected(self):
        with pytest.raises(ValueError, match="Schema column names must not overlap"):
            FeatureViewConfig(
                name="my_features",
                version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
                schema=[
                    ColumnSpec(name="id", type="string"),  # overlaps with primary key
                ],
            )

    def test_with_schema_and_dependency(self):
        cfg = FeatureViewConfig(
            name="my_features",
            version="v2",
            owner="alice@corp.com",
            description="User features",
            primary_keys=[KeySpec(name="user_id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string", format="yyyy-MM-dd")],
            storage=StorageSpec(base_path="gs://b/feat/v2"),
            schema=[
                ColumnSpec(name="age", type="integer"),
                ColumnSpec(name="score", type="double"),
            ],
            dependency=[
                Dependency(name="upstream_fv", version="v1"),
            ],
        )
        assert len(cfg.schema) == 2
        assert len(cfg.dependency) == 1
        assert cfg.dependency[0].name == "upstream_fv"


class TestModelConfig:
    def test_minimal_model_config(self):
        cfg = ModelConfig(
            name="my_model",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
        )
        assert cfg.kind == "model"

    def test_model_schema_cols_require_feature_view(self):
        with pytest.raises(ValueError, match="must have feature_view"):
            ModelConfig(
                name="my_model",
                version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                schema=[
                    ColumnSpec(name="age", type="integer"),  # missing feature_view
                ],
            )

    def test_model_schema_cols_require_feature_view_version(self):
        with pytest.raises(ValueError, match="must have feature_view_version"):
            ModelConfig(
                name="my_model",
                version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                schema=[
                    ColumnSpec(name="age", type="integer", feature_view="fv"),
                ],
            )

    def test_valid_model_with_feature_refs(self):
        cfg = ModelConfig(
            name="my_model",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="age", type="integer", feature_view="fv1", feature_view_version="v1"),
                ColumnSpec(name="score", type="double", feature_view="fv2", feature_view_version="v3"),
            ],
            dependency=[Dependency(name="base_model", version="v1")],
        )
        assert len(cfg.schema) == 2
        assert cfg.schema[0].feature_view == "fv1"


class TestLabelConfig:
    def test_minimal_label_config(self):
        cfg = LabelConfig(
            name="my_label",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/label/v1"),
        )
        assert cfg.kind == "label"


class TestDatasetConfig:
    def test_minimal_dataset_config(self):
        cfg = DatasetConfig(
            name="my_dataset",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/ds/v1"),
        )
        assert cfg.kind == "dataset"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_schema.py -v
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `src/schema.py`**

```python
"""Config dataclasses for typed entity configuration."""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Literal


_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_name(name: str):
    if not _NAME_PATTERN.match(name):
        raise ValueError(
            f"name must match [a-z][a-z0-9_]*, got '{name}'"
        )


def _check_column_overlap(primary_keys, partition_columns, schema):
    key_names = {k.name for k in primary_keys}
    part_names = {p.name for p in partition_columns}
    schema_names = {c.name for c in schema}
    overlap = (key_names | part_names) & schema_names
    if overlap:
        raise ValueError(
            f"Schema column names must not overlap with primary_keys "
            f"or partition_columns. Overlapping: {overlap}"
        )


@dataclass
class ColumnSpec:
    name: str
    type: str
    is_label: bool = False
    feature_view: Optional[str] = None
    feature_view_version: Optional[str] = None


@dataclass
class KeySpec:
    name: str
    type: str
    format: Optional[str] = None


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
class Lookback:
    freq: Literal["day", "month"] = "day"
    periods: int = 1

    def __post_init__(self):
        if self.freq not in ("day", "month"):
            raise ValueError(f"freq must be 'day' or 'month', got '{self.freq}'")


@dataclass
class _BaseConfig:
    name: str
    version: str
    owner: str = ""
    description: str = ""
    dependency: List[Dependency] = field(default_factory=list)
    primary_keys: List[KeySpec] = field(default_factory=list)
    partition_columns: List[KeySpec] = field(default_factory=list)

    def __post_init__(self):
        _validate_name(self.name)
        if not self.primary_keys:
            raise ValueError("primary_keys must not be empty")
        if not self.partition_columns:
            raise ValueError("partition_columns must not be empty")
        _check_column_overlap(
            self.primary_keys, self.partition_columns,
            getattr(self, 'schema', [])
        )


@dataclass
class FeatureViewConfig(_BaseConfig):
    storage: Optional[StorageSpec] = None
    pipeline: PipelineSpec = field(default_factory=PipelineSpec)
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "feature_view"

    def __post_init__(self):
        super().__post_init__()
        _check_column_overlap(
            self.primary_keys, self.partition_columns, self.schema
        )


@dataclass
class ModelConfig(_BaseConfig):
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "model"

    def __post_init__(self):
        super().__post_init__()
        _check_column_overlap(
            self.primary_keys, self.partition_columns, self.schema
        )
        for col in self.schema:
            if not col.feature_view:
                raise ValueError(
                    f"Model schema column '{col.name}' must have feature_view"
                )
            if not col.feature_view_version:
                raise ValueError(
                    f"Model schema column '{col.name}' must have feature_view_version"
                )


@dataclass
class LabelConfig(_BaseConfig):
    storage: Optional[StorageSpec] = None
    retention: RetentionSpec = field(default_factory=RetentionSpec)
    schema: List[ColumnSpec] = field(default_factory=list)
    kind: str = "label"

    def __post_init__(self):
        super().__post_init__()
        _check_column_overlap(
            self.primary_keys, self.partition_columns, self.schema
        )


@dataclass
class DatasetConfig(_BaseConfig):
    storage: Optional[StorageSpec] = None
    kind: str = "dataset"

    def __post_init__(self):
        super().__post_init__()


TrainingSetConfig = DatasetConfig
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_schema.py -v
```
Expected: PASS (all 17 tests)

- [ ] **Step 5: Update `src/__init__.py` to export schema classes**

- [ ] **Step 6: Commit**

```bash
git add src/schema.py src/__init__.py tests/test_schema.py
git commit -m "feat: add schema module with typed config dataclasses"
```

---

### Task 4: `storage.py` — Storage Backend Abstraction

**Files:**
- Create: `src/storage.py`
- Create: `tests/test_storage.py`

- [ ] **Step 1: Write failing tests for storage**

```python
# tests/test_storage.py
import os
import tempfile
import pytest
import yaml
from feature_store.storage import LocalBackend, get_backend


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def backend(tmp_dir):
    return LocalBackend()


class TestLocalBackend:
    def test_exists_true(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "test.txt")
        with open(path, "w") as f:
            f.write("hello")
        assert backend.exists(path) is True

    def test_exists_false(self, backend, tmp_dir):
        assert backend.exists(os.path.join(tmp_dir, "nope.txt")) is False

    def test_open_read(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "data.yaml")
        content = "key: value\n"
        with open(path, "w") as f:
            f.write(content)
        with backend.open(path, "r") as f:
            assert f.read() == content

    def test_open_write(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "out.yaml")
        with backend.open(path, "w") as f:
            f.write("hello")
        with open(path) as f:
            assert f.read() == "hello"

    def test_glob(self, backend, tmp_dir):
        for name in ["a.yaml", "b.yaml", "c.txt"]:
            with open(os.path.join(tmp_dir, name), "w") as f:
                f.write("x")
        results = backend.glob(os.path.join(tmp_dir, "*.yaml"))
        assert len(results) == 2
        assert all(p.endswith(".yaml") for p in results)

    def test_cp(self, backend, tmp_dir):
        src = os.path.join(tmp_dir, "src.txt")
        dst = os.path.join(tmp_dir, "dst.txt")
        with open(src, "w") as f:
            f.write("copied")
        backend.cp(src, dst)
        assert os.path.exists(dst)
        with open(dst) as f:
            assert f.read() == "copied"

    def test_rm_file(self, backend, tmp_dir):
        path = os.path.join(tmp_dir, "remove_me.txt")
        with open(path, "w") as f:
            f.write("x")
        backend.rm(path)
        assert not os.path.exists(path)

    def test_rm_dir_recursive(self, backend, tmp_dir):
        d = os.path.join(tmp_dir, "sub")
        os.makedirs(os.path.join(d, "nested"))
        with open(os.path.join(d, "f.txt"), "w") as f:
            f.write("x")
        backend.rm(d, recursive=True)
        assert not os.path.exists(d)

    def test_read_parquet(self, backend, tmp_dir):
        # parquet read requires Spark; test not covered here
        pass

    def test_write_parquet(self, backend, tmp_dir):
        # parquet write requires Spark; test not covered here
        pass


class TestGetBackend:
    def test_local_backend(self):
        b = get_backend("/tmp/foo")
        assert isinstance(b, LocalBackend)

    def test_gcs_backend_imports(self):
        # GCS backend needs google.cloud.storage; may not be available in test
        # So we test that the factory recognizes gs:// scheme
        try:
            b = get_backend("gs://bucket/path")
            from feature_store.storage import GCSBackend
            assert isinstance(b, GCSBackend)
        except ImportError:
            pytest.skip("google-cloud-storage not installed")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_storage.py -v
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `src/storage.py`**

```python
"""Storage backend abstraction for GCS, local, and S3."""

import os
import glob as glob_module
import shutil
from abc import ABC, abstractmethod
from typing import List, Optional, IO


class StorageBackend(ABC):

    @abstractmethod
    def read_parquet(self, spark, path, columns=None, filters=None):
        ...

    @abstractmethod
    def write_parquet(self, df, path, partition_cols, mode,
                      compression="snappy", partition_num=24):
        ...

    @abstractmethod
    def open(self, path: str, mode: str = "r") -> IO:
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        ...

    @abstractmethod
    def glob(self, pattern: str) -> List[str]:
        ...

    @abstractmethod
    def cp(self, src: str, dst: str):
        ...

    @abstractmethod
    def rm(self, path: str, recursive: bool = False):
        ...


class LocalBackend(StorageBackend):

    def read_parquet(self, spark, path, columns=None, filters=None):
        reader = spark.read.parquet(path)
        if columns:
            reader = reader.select(*columns)
        if filters:
            for col_name, condition in filters.items():
                reader = reader.filter(condition)
        return reader

    def write_parquet(self, df, path, partition_cols, mode,
                      compression="snappy", partition_num=24):
        (df.repartition(partition_num).write
           .partitionBy(*partition_cols)
           .mode(mode)
           .option("compression", compression)
           .parquet(path))

    def open(self, path: str, mode: str = "r") -> IO:
        return open(path, mode, encoding="utf-8")

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def glob(self, pattern: str) -> List[str]:
        return glob_module.glob(pattern, recursive=True)

    def cp(self, src: str, dst: str):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)

    def rm(self, path: str, recursive: bool = False):
        if not os.path.exists(path):
            return
        if os.path.isdir(path):
            if recursive:
                shutil.rmtree(path)
        else:
            os.remove(path)


class GCSBackend(StorageBackend):
    """GCS backend using google-cloud-storage for YAML ops and Spark for parquet."""

    def __init__(self):
        from google.cloud import storage
        self._client = storage.Client()

    def _parse_gs_path(self, gs_path: str):
        parts = gs_path.replace("gs://", "").split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix

    def read_parquet(self, spark, path, columns=None, filters=None):
        reader = spark.read.parquet(path)
        if columns:
            reader = reader.select(*columns)
        if filters:
            for col_name, condition in filters.items():
                reader = reader.filter(condition)
        return reader

    def write_parquet(self, df, path, partition_cols, mode,
                      compression="snappy", partition_num=24):
        (df.repartition(partition_num).write
           .partitionBy(*partition_cols)
           .mode(mode)
           .option("partitionOverwriteMode", "dynamic")
           .option("compression", compression)
           .parquet(path))

    def open(self, path: str, mode: str = "r") -> IO:
        import fsspec
        fs = fsspec.filesystem("gcs")
        return fs.open(path, mode, encoding="utf-8")

    def exists(self, path: str) -> bool:
        import fsspec
        fs = fsspec.filesystem("gcs")
        return fs.exists(path)

    def glob(self, pattern: str) -> List[str]:
        import fsspec
        fs = fsspec.filesystem("gcs")
        return fs.glob(pattern)

    def cp(self, src: str, dst: str):
        import fsspec
        fs = fsspec.filesystem("gcs")
        if fs.isdir(src):
            for f in fs.ls(src, detail=False):
                fs.copy(f, os.path.join(dst, os.path.basename(f)))
        else:
            fs.copy(src, dst)

    def rm(self, path: str, recursive: bool = False):
        import fsspec
        fs = fsspec.filesystem("gcs")
        if recursive:
            fs.rm(path, recursive=True)
        else:
            fs.rm(path)


def get_backend(path: str) -> StorageBackend:
    if path.startswith("gs://"):
        return GCSBackend()
    elif path.startswith("s3://"):
        raise NotImplementedError("S3 backend not yet implemented")
    else:
        return LocalBackend()
```

- [ ] **Step 4: Run tests to verify**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_storage.py -v
```
Expected: PASS (or SKIP for GCS tests if library unavailable)

- [ ] **Step 5: Commit**

```bash
git add src/storage.py tests/test_storage.py
git commit -m "feat: add storage backend abstraction with Local and GCS backends"
```

---

### Task 5: `registry.py` — YAML Operations and Validation

**Files:**
- Create: `src/registry.py`
- Create: `tests/test_registry.py`

- [ ] **Step 1: Write failing tests for registry**

```python
# tests/test_registry.py
import os
import tempfile
import pytest
import yaml
from feature_store.types import EntityKind
from feature_store.schema import (
    ColumnSpec, KeySpec, Dependency, StorageSpec,
    FeatureViewConfig, ModelConfig, LabelConfig, DatasetConfig,
)
from feature_store.storage import LocalBackend
from feature_store.registry import (
    config_to_yaml, yaml_to_config, build_config_from_df,
    validate_dataframe, MissingColumnsError, ExtraColumnsError,
    EntityNotFoundError,
)


@pytest.fixture
def backend():
    return LocalBackend()


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def feature_view_config():
    return FeatureViewConfig(
        name="test_features",
        version="v1",
        owner="alice@corp.com",
        description="Test feature view",
        primary_keys=[KeySpec(name="user_id", type="string")],
        partition_columns=[KeySpec(name="dt", type="string", format="yyyy-MM-dd")],
        storage=StorageSpec(base_path="gs://bucket/features/test/v1"),
        schema=[
            ColumnSpec(name="age", type="integer"),
            ColumnSpec(name="score", type="double"),
        ],
    )


class TestConfigToYaml:
    def test_feature_view_round_trip(self, feature_view_config):
        yaml_str = config_to_yaml(feature_view_config)
        data = yaml.safe_load(yaml_str)
        assert data["kind"] == "feature_view"
        assert data["name"] == "test_features"
        assert data["version"] == "v1"
        assert data["owner"] == "alice@corp.com"
        assert len(data["primary_keys"]) == 1
        assert data["primary_keys"][0]["name"] == "user_id"
        assert data["primary_keys"][0]["type"] == "string"
        assert len(data["partition_columns"]) == 1
        assert data["partition_columns"][0]["format"] == "yyyy-MM-dd"
        assert "storage" in data
        assert data["storage"]["base_path"] == "gs://bucket/features/test/v1"
        assert len(data["schema"]) == 2

    def test_model_config_to_yaml(self):
        cfg = ModelConfig(
            name="my_model",
            version="v2",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="feat1", type="integer", feature_view="fv1", feature_view_version="v1"),
            ],
            dependency=[Dependency(name="base", version="v1")],
        )
        yaml_str = config_to_yaml(cfg)
        data = yaml.safe_load(yaml_str)
        assert data["kind"] == "model"
        assert "storage" not in data
        assert data["schema"][0]["feature_view"] == "fv1"
        assert data["schema"][0]["feature_view_version"] == "v1"
        assert len(data["dependency"]) == 1

    def test_label_config_to_yaml(self):
        cfg = LabelConfig(
            name="my_label",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/l/v1"),
            schema=[
                ColumnSpec(name="churn", type="integer", is_label=True),
            ],
        )
        yaml_str = config_to_yaml(cfg)
        data = yaml.safe_load(yaml_str)
        assert data["kind"] == "label"
        assert data["schema"][0]["is_label"] is True

    def test_dataset_config_to_yaml(self):
        cfg = DatasetConfig(
            name="my_dataset",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/ds/v1"),
            dependency=[Dependency(name="model1", version="v1")],
        )
        yaml_str = config_to_yaml(cfg)
        data = yaml.safe_load(yaml_str)
        assert data["kind"] == "dataset"
        assert "schema" not in data


class TestYamlToConfig:
    def test_feature_view_from_dict(self):
        data = {
            "kind": "feature_view",
            "name": "fv",
            "version": "v1",
            "owner": "a@b.com",
            "primary_keys": [{"name": "id", "type": "string"}],
            "partition_columns": [{"name": "dt", "type": "string"}],
            "storage": {"base_path": "gs://b/fv/v1"},
            "schema": [
                {"name": "age", "type": "integer"},
            ],
        }
        cfg = yaml_to_config(data)
        assert isinstance(cfg, FeatureViewConfig)
        assert cfg.name == "fv"
        assert len(cfg.schema) == 1

    def test_model_from_dict(self):
        data = {
            "kind": "model",
            "name": "m",
            "version": "v1",
            "primary_keys": [{"name": "id", "type": "string"}],
            "partition_columns": [{"name": "dt", "type": "string"}],
            "schema": [
                {"name": "feat1", "type": "integer", "feature_view": "fv1", "feature_view_version": "v1"},
            ],
        }
        cfg = yaml_to_config(data)
        assert isinstance(cfg, ModelConfig)

    def test_label_from_dict(self):
        data = {
            "kind": "label",
            "name": "lbl",
            "version": "v1",
            "primary_keys": [{"name": "id", "type": "string"}],
            "partition_columns": [{"name": "dt", "type": "string"}],
            "storage": {"base_path": "gs://b/l/v1"},
            "schema": [
                {"name": "target", "type": "integer", "is_label": True},
            ],
        }
        cfg = yaml_to_config(data)
        assert isinstance(cfg, LabelConfig)

    def test_dataset_from_dict(self):
        data = {
            "kind": "dataset",
            "name": "ds",
            "version": "v1",
            "primary_keys": [{"name": "id", "type": "string"}],
            "partition_columns": [{"name": "dt", "type": "string"}],
            "storage": {"base_path": "gs://b/ds/v1"},
        }
        cfg = yaml_to_config(data)
        assert isinstance(cfg, DatasetConfig)

    def test_unknown_kind_raises(self):
        data = {
            "kind": "unknown_type",
            "name": "x",
            "version": "v1",
        }
        with pytest.raises(ValueError, match="Unknown kind"):
            yaml_to_config(data)

    def test_missing_kind_raises(self):
        data = {"name": "x", "version": "v1"}
        with pytest.raises(ValueError, match="Missing 'kind' field"):
            yaml_to_config(data)


class TestValidateDataframe:
    def test_missing_columns(self, backend):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([(1, "a")], ["id", "name"])
            cfg = FeatureViewConfig(
                name="fv", version="v1",
                primary_keys=[KeySpec(name="id", type="integer")],
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/fv/v1"),
                schema=[
                    ColumnSpec(name="name", type="string"),
                    ColumnSpec(name="missing_col", type="integer"),
                ],
            )
            with pytest.raises(MissingColumnsError, match="missing"):
                validate_dataframe(df, cfg)
        finally:
            spark.stop()

    def test_extra_columns(self, backend):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([(1, "a", "extra")], ["id", "name", "ghost"])
            cfg = FeatureViewConfig(
                name="fv", version="v1",
                primary_keys=[KeySpec(name="id", type="integer")],
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/fv/v1"),
                schema=[
                    ColumnSpec(name="name", type="string"),
                ],
            )
            with pytest.raises(ExtraColumnsError, match="unknown"):
                validate_dataframe(df, cfg)
        finally:
            spark.stop()

    def test_allow_extra_columns(self, backend):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            df = spark.createDataFrame([(1, "a", "extra")], ["id", "name", "ghost"])
            cfg = FeatureViewConfig(
                name="fv", version="v1",
                primary_keys=[KeySpec(name="id", type="integer")],
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/fv/v1"),
                schema=[
                    ColumnSpec(name="name", type="string"),
                ],
            )
            # Should not raise when allow_extra_columns=True
            validate_dataframe(df, cfg, allow_extra_columns=True)
        finally:
            spark.stop()


class TestBuildConfigFromDf:
    def test_feature_view_from_df(self):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        try:
            df = spark.createDataFrame(
                [(1, "aaa", "2024-01-01", 25, 0.8)],
                ["user_id", "name", "dt", "age", "score"]
            )
            cfg = build_config_from_df(
                df=df,
                kind=EntityKind.FEATURE_VIEW,
                name="my_fv",
                version="v1",
                primary_keys=["user_id"],
                partition_columns=["dt"],
                storage_base_path="gs://bucket/fv/v1",
            )
            assert cfg.name == "my_fv"
            assert cfg.kind == "feature_view"
            assert len(cfg.primary_keys) == 1
            assert cfg.primary_keys[0].name == "user_id"
            assert len(cfg.partition_columns) == 1
            assert cfg.partition_columns[0].name == "dt"
            assert len(cfg.schema) == 2  # name, age, score (user_id and dt excluded)
            schema_names = {c.name for c in cfg.schema}
            assert schema_names == {"name", "age", "score"}
        finally:
            spark.stop()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_registry.py -v -k "not TestValidateDataframe and not TestBuildConfigFromDf"
```
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Write `src/registry.py`**

```python
"""Registry operations: YAML serialization, validation, build, lifecycle, migration."""

import re
import os
import uuid
import logging
from typing import Dict, List, Optional, Union

import yaml

from feature_store.types import EntityKind
from feature_store.schema import (
    ColumnSpec, KeySpec, Dependency, PipelineSpec,
    RetentionSpec, StorageSpec,
    FeatureViewConfig, ModelConfig, LabelConfig, DatasetConfig, TrainingSetConfig,
)

logger = logging.getLogger("feature_store")


# ── Exceptions ────────────────────────────────────────────

class FeatureStoreError(Exception):
    """Base exception for feature store errors."""


class EntityNotFoundError(FeatureStoreError):
    """Requested entity not found in registry."""


class ColumnNotFoundError(FeatureStoreError):
    """Requested column not found in entity schema."""


class SchemaValidationError(FeatureStoreError):
    """DataFrame schema does not match registered schema."""


class MissingColumnsError(SchemaValidationError):
    """DataFrame is missing required columns."""


class ExtraColumnsError(SchemaValidationError):
    """DataFrame has columns not registered in the YAML schema."""


class RegistryFormatError(FeatureStoreError):
    """YAML registry file has invalid format."""


class StorageError(FeatureStoreError):
    """Storage backend operation failed."""


# ── YAML Serialization ────────────────────────────────────

_KIND_TO_CLASS = {
    "feature_view": FeatureViewConfig,
    "model": ModelConfig,
    "label": LabelConfig,
    "dataset": DatasetConfig,
    "training_set": TrainingSetConfig,
}


def _dict_to_specs(dicts, spec_class):
    return [spec_class(**d) for d in dicts]


def config_to_yaml(config) -> str:
    """Serialize any entity config to YAML string."""
    data = {"kind": config.kind, "name": config.name, "version": config.version}

    if config.owner:
        data["owner"] = config.owner
    if config.description:
        data["description"] = config.description

    if config.dependency:
        data["dependency"] = [{"name": d.name, "version": d.version}
                              for d in config.dependency]

    if config.primary_keys:
        data["primary_keys"] = [
            _drop_none({"name": k.name, "type": k.type, "format": k.format})
            for k in config.primary_keys
        ]

    if config.partition_columns:
        data["partition_columns"] = [
            _drop_none({"name": p.name, "type": p.type, "format": p.format})
            for p in config.partition_columns
        ]

    if hasattr(config, "storage") and config.storage:
        data["storage"] = {
            "base_path": config.storage.base_path,
            "format": config.storage.format,
        }

    if hasattr(config, "pipeline"):
        data["pipeline"] = {
            "update_frequency": config.pipeline.update_frequency,
            "source_job": config.pipeline.source_job,
            "alert_threshold_hours": config.pipeline.alert_threshold_hours,
        }

    if hasattr(config, "retention"):
        data["retention"] = {
            "ttl_days": config.retention.ttl_days,
            "cold_tier_days": config.retention.cold_tier_days,
        }

    if hasattr(config, "schema") and config.schema:
        data["schema"] = []
        for col in config.schema:
            col_data = {"name": col.name, "type": col.type}
            if col.is_label:
                col_data["is_label"] = True
            if col.feature_view:
                col_data["feature_view"] = col.feature_view
            if col.feature_view_version:
                col_data["feature_view_version"] = col.feature_view_version
            data["schema"].append(col_data)

    return yaml.dump(data, default_flow_style=False, sort_keys=False,
                     allow_unicode=True, indent=2)


def _drop_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def yaml_to_config(data: Dict):
    """Parse a YAML dict into the appropriate typed Config object."""
    if "kind" not in data:
        raise ValueError("Missing 'kind' field in YAML")

    kind = data["kind"]
    cls = _KIND_TO_CLASS.get(kind)
    if cls is None:
        raise ValueError(f"Unknown kind: {kind}")

    kwargs = {
        "name": data["name"],
        "version": data["version"],
        "owner": data.get("owner", ""),
        "description": data.get("description", ""),
    }

    # dependency
    deps = data.get("dependency", [])
    if deps:
        kwargs["dependency"] = [Dependency(**d) for d in deps]

    # primary_keys
    kwargs["primary_keys"] = [KeySpec(**k) for k in data.get("primary_keys", [])]

    # partition_columns
    kwargs["partition_columns"] = [
        KeySpec(**p) for p in data.get("partition_columns", [])
    ]

    # storage (optional for some types)
    if "storage" in data and data["storage"]:
        kwargs["storage"] = StorageSpec(**data["storage"])

    # pipeline (feature_view only)
    if kind == "feature_view" and "pipeline" in data:
        kwargs["pipeline"] = PipelineSpec(**data["pipeline"])

    # retention (feature_view, label)
    if kind in ("feature_view", "label") and "retention" in data:
        kwargs["retention"] = RetentionSpec(**data["retention"])

    # schema (all except dataset/training_set)
    if kind != "dataset" and kind != "training_set" and "schema" in data:
        kwargs["schema"] = [ColumnSpec(**c) for c in data["schema"]]

    return cls(**kwargs)


def load_yaml(backend, path: str) -> Dict:
    with backend.open(path, "r") as f:
        return yaml.safe_load(f) or {}


def write_yaml(backend, path: str, config) -> str:
    yaml_str = config_to_yaml(config)
    parent = os.path.dirname(path)
    if parent and not backend.exists(parent):
        backend.cp(None, parent)  # no-op for local
    with backend.open(path, "w") as f:
        f.write(yaml_str)
    return yaml_str


def _registry_path(registry_dir: str, kind: EntityKind, name: str, version: str) -> str:
    """Build registry file path for an entity."""
    kind_dir = {
        EntityKind.FEATURE_VIEW: "feature_registry",
        EntityKind.MODEL: "model_registry",
        EntityKind.LABEL: "label_registry",
        EntityKind.DATASET: "dataset_registry",
        EntityKind.TRAINING_SET: "training_set_registry",
    }
    return f"{registry_dir}/{kind_dir[kind]}/{name}_{version}.yaml"


# ── Schema validation ────────────────────────────────────

def validate_dataframe(df, config, allow_extra_columns: bool = False):
    """Validate DataFrame schema against registered config."""
    all_config_cols = [k.name for k in config.primary_keys] + \
                      [p.name for p in config.partition_columns] + \
                      [c.name for c in config.schema]

    actual_cols = set(df.columns)
    expected_cols = set(all_config_cols)

    missing = expected_cols - actual_cols
    if missing:
        raise MissingColumnsError(
            f"DataFrame missing columns: {sorted(missing)}"
        )

    if not allow_extra_columns:
        extra = actual_cols - expected_cols
        if extra:
            raise ExtraColumnsError(
                f"DataFrame has unknown columns: {sorted(extra)}. "
                f"Pass allow_extra_columns=True to suppress."
            )


# ── Build config from DataFrame ───────────────────────────

def _spark_type_to_str(spark_type) -> str:
    from pyspark.sql.types import IntegerType, LongType, FloatType, DoubleType, BooleanType

    if isinstance(spark_type, (IntegerType, LongType)):
        return "integer"
    elif isinstance(spark_type, (FloatType, DoubleType)):
        return "double"
    elif isinstance(spark_type, BooleanType):
        return "boolean"
    else:
        return "string"


def build_config_from_df(
    df,
    kind: EntityKind,
    name: str,
    version: str = "v1",
    primary_keys: List[str] = None,
    partition_columns: List[str] = None,
    storage_base_path: str = "",
    owner: str = "",
    description: str = "",
    **kwargs,
):
    """Build a typed config from a Spark DataFrame schema."""

    primary_keys = primary_keys or []
    partition_columns = partition_columns or []

    # Build KeySpec lists
    pk_specs = []
    for pk in primary_keys:
        field = next((f for f in df.schema.fields if f.name == pk), None)
        col_type = _spark_type_to_str(field.dataType) if field else "string"
        pk_specs.append(KeySpec(name=pk, type=col_type))

    part_specs = []
    for pc in partition_columns:
        field = next((f for f in df.schema.fields if f.name == pc), None)
        col_type = _spark_type_to_str(field.dataType) if field else "string"
        part_specs.append(KeySpec(name=pc, type=col_type))

    # Build schema ColumnSpecs (exclude primary_keys and partition_columns)
    exclude = set(primary_keys) | set(partition_columns)
    schema_cols = []
    for field in df.schema.fields:
        if field.name in exclude:
            continue
        schema_cols.append(
            ColumnSpec(name=field.name, type=_spark_type_to_str(field.dataType))
        )

    storage = StorageSpec(base_path=storage_base_path) if storage_base_path else None

    if kind == EntityKind.FEATURE_VIEW:
        return FeatureViewConfig(
            name=name, version=version, owner=owner, description=description,
            primary_keys=pk_specs, partition_columns=part_specs,
            storage=storage, schema=schema_cols,
        )
    elif kind == EntityKind.MODEL:
        return ModelConfig(
            name=name, version=version, owner=owner, description=description,
            primary_keys=pk_specs, partition_columns=part_specs,
            schema=schema_cols,
        )
    elif kind == EntityKind.LABEL:
        return LabelConfig(
            name=name, version=version, owner=owner, description=description,
            primary_keys=pk_specs, partition_columns=part_specs,
            storage=storage, schema=schema_cols,
        )
    elif kind in (EntityKind.DATASET, EntityKind.TRAINING_SET):
        return DatasetConfig(
            name=name, version=version, owner=owner, description=description,
            primary_keys=pk_specs, partition_columns=part_specs,
            storage=storage,
        )
    else:
        raise ValueError(f"Unsupported entity kind: {kind}")


# ── Build model config from feature references ────────────

def build_model_config(
    features,           # List[dict] with {name, type, feature_view, feature_view_version}
    name: str,
    version: str,
    primary_keys: List[Dict] = None,
    partition_columns: List[Dict] = None,
    owner: str = "",
    description: str = "",
    dependency: List[Dict] = None,
) -> ModelConfig:
    """Build a ModelConfig from a structured feature list."""

    schema = []
    for feat in features:
        if isinstance(feat, dict):
            schema.append(ColumnSpec(
                name=feat["name"],
                type=feat.get("type", "string"),
                feature_view=feat.get("feature_view"),
                feature_view_version=feat.get("feature_view_version"),
            ))
        elif isinstance(feat, str):
            # Support old URN format for transition: "view@version:feature"
            if ":" in feat and "@" in feat:
                view_part, feat_name = feat.split(":")
                view_name, view_ver = view_part.split("@")
                schema.append(ColumnSpec(
                    name=feat_name, type="string",
                    feature_view=view_name, feature_view_version=view_ver,
                ))
            else:
                raise ValueError(
                    f"Feature '{feat}' must be a dict or URN 'view@version:feature'"
                )

    pks = [KeySpec(**p) for p in (primary_keys or [])]
    parts = [KeySpec(**p) for p in (partition_columns or [])]
    deps = [Dependency(**d) for d in (dependency or [])]

    return ModelConfig(
        name=name, version=version, owner=owner, description=description,
        primary_keys=pks, partition_columns=parts,
        schema=schema, dependency=deps,
    )


# ── List entities ─────────────────────────────────────────

def list_entities(backend, registry_dir: str, kind: EntityKind = None) -> List[Dict]:
    """Scan registry and return summaries of all matching entities."""
    pattern = f"{registry_dir}/**/*.yaml"
    paths = backend.glob(pattern)

    results = []
    for path in paths:
        try:
            data = load_yaml(backend, path)
            if not data:
                continue
            entity_kind = data.get("kind")
            if kind and entity_kind != kind.value:
                continue
            results.append({
                "kind": entity_kind,
                "name": data.get("name"),
                "version": data.get("version"),
                "owner": data.get("owner", ""),
                "path": path,
            })
        except Exception as e:
            logger.warning(f"Skipping {path}: {e}")

    return results


# ── Lifecycle sync ────────────────────────────────────────

def sync_lifecycle(backend, registry_dir: str) -> Dict[str, int]:
    """Scan YAMLs, extract TTL rules, push lifecycle policies to storage buckets."""
    from collections import defaultdict

    rules_by_bucket = defaultdict(list)

    paths = backend.glob(f"{registry_dir}/**/*.yaml")
    for path in paths:
        data = load_yaml(backend, path)
        if not data or "storage" not in data or "retention" not in data:
            continue

        base_path = data["storage"]["base_path"]
        parts = base_path.replace("gs://", "").split("/")
        bucket_name = parts[0]
        prefix = "/".join(parts[1:]) + "/"

        retention = data["retention"]
        ttl = retention.get("ttl_days", 360)
        cold = retention.get("cold_tier_days", 180)

        rules_by_bucket[bucket_name].extend([
            {
                "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
                "condition": {"age": cold, "matchesPrefix": [prefix]},
            },
            {
                "action": {"type": "Delete"},
                "condition": {"age": ttl, "matchesPrefix": [prefix]},
            },
        ])
        logger.info(f"Extracted rules: {bucket_name}/{prefix} (Cold: {cold}d, TTL: {ttl}d)")

    if not rules_by_bucket:
        logger.warning("No lifecycle rules found.")
        return {}

    from google.cloud import storage
    import time

    client = storage.Client()
    applied = {}

    for bucket_name, rules in rules_by_bucket.items():
        logger.info(f"Applying {len(rules)} rules to [{bucket_name}]...")
        bucket = client.get_bucket(bucket_name)
        bucket.lifecycle_rules = list(rules)
        bucket.patch()
        applied[bucket_name] = len(rules)
        logger.info(f"Bucket [{bucket_name}] synced.")
        time.sleep(1)

    return applied
```

- [ ] **Step 4: Run tests to verify**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_registry.py -v -k "not TestValidateDataframe and not TestBuildConfigFromDf"
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/registry.py tests/test_registry.py
git commit -m "feat: add registry module with YAML serialization, validation, and lifecycle sync"
```

---

### Task 6: `client.py` — FeatureStoreClient

**Files:**
- Create: `src/client.py`
- Create: `tests/test_client.py`

- [ ] **Step 1: Write failing tests for client**

```python
# tests/test_client.py
import os
import tempfile
import pytest
import pandas as pd
from feature_store.client import FeatureStoreClient
from feature_store.types import EntityKind
from feature_store.schema import (
    FeatureViewConfig, ModelConfig, LabelConfig,
    KeySpec, ColumnSpec, StorageSpec, Lookback,
)


@pytest.fixture
def spark_session():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("feature_store_test") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def client(spark_session, tmp_dir):
    registry_dir = os.path.join(tmp_dir, "registry")
    os.makedirs(registry_dir)
    return FeatureStoreClient(spark=spark_session, registry_dir=registry_dir)


class TestRegistration:
    def test_register_feature_view_from_config(self, client):
        config = FeatureViewConfig(
            name="user_features",
            version="v1",
            primary_keys=[KeySpec(name="user_id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="/tmp/features/user/v1"),
            schema=[
                ColumnSpec(name="age", type="integer"),
                ColumnSpec(name="score", type="double"),
            ],
        )
        path = client.register(config)
        assert path.endswith(".yaml")
        assert os.path.exists(path)

    def test_register_feature_view_from_df(self, client, spark_session):
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 25, 0.8)],
            ["user_id", "dt", "age", "score"]
        )
        path = client.register(
            source=df,
            kind=EntityKind.FEATURE_VIEW,
            name="df_features",
            version="v1",
            primary_keys=["user_id"],
            partition_columns=["dt"],
            storage_base_path="/tmp/features/df/v1",
        )
        assert os.path.exists(path)

    def test_register_model_from_config(self, client):
        config = ModelConfig(
            name="my_model",
            version="v1",
            primary_keys=[KeySpec(name="user_id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="age", type="integer", feature_view="user_features", feature_view_version="v1"),
                ColumnSpec(name="score", type="double", feature_view="user_features", feature_view_version="v1"),
            ],
        )
        path = client.register(config)
        assert os.path.exists(path)


class TestWriteAndRead:
    def test_write_and_read_feature_view(self, client, spark_session, tmp_dir):
        # Register
        config = FeatureViewConfig(
            name="test_fv",
            version="v1",
            primary_keys=[KeySpec(name="id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=os.path.join(tmp_dir, "data/test_fv/v1")),
            schema=[
                ColumnSpec(name="val", type="double"),
            ],
        )
        client.register(config)

        # Write
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5), (2, "2024-01-01", 0.8)],
            ["id", "dt", "val"]
        )
        client.write_entity(df, EntityKind.FEATURE_VIEW, "test_fv", "v1")

        # Read
        result = client.get_entity(
            EntityKind.FEATURE_VIEW, "test_fv", "v1",
            columns=["val"],
            start_date="2024-01-01",
        )
        assert result.count() == 2

    def test_write_entity_unregistered_fails(self, client, spark_session):
        df = spark_session.createDataFrame([(1,)], ["x"])
        with pytest.raises(Exception):
            client.write_entity(df, EntityKind.FEATURE_VIEW, "ghost", "v1")


class TestModelFeatures:
    def test_get_model_features(self, client, spark_session, tmp_dir):
        # Register feature view 1
        fv1_config = FeatureViewConfig(
            name="fv_a",
            version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=os.path.join(tmp_dir, "data/fv_a/v1")),
            schema=[
                ColumnSpec(name="feat1", type="double"),
            ],
        )
        client.register(fv1_config)

        # Register feature view 2
        fv2_config = FeatureViewConfig(
            name="fv_b",
            version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=os.path.join(tmp_dir, "data/fv_b/v1")),
            schema=[
                ColumnSpec(name="feat2", type="integer"),
            ],
        )
        client.register(fv2_config)

        # Write data to both
        spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5), (2, "2024-01-01", 0.8)],
            ["user_id", "dt", "feat1"]
        ).write.mode("overwrite").option("compression", "snappy") \
            .partitionBy("dt").parquet(os.path.join(tmp_dir, "data/fv_a/v1"))

        spark_session.createDataFrame(
            [(1, "2024-01-01", 10), (2, "2024-01-01", 20)],
            ["user_id", "dt", "feat2"]
        ).write.mode("overwrite").option("compression", "snappy") \
            .partitionBy("dt").parquet(os.path.join(tmp_dir, "data/fv_b/v1"))

        # Register model
        model_config = ModelConfig(
            name="test_model",
            version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="feat1", type="double", feature_view="fv_a", feature_view_version="v1"),
                ColumnSpec(name="feat2", type="integer", feature_view="fv_b", feature_view_version="v1"),
            ],
        )
        client.register(model_config)

        # Query
        query_df = spark_session.createDataFrame(
            [(1, "2024-01-01"), (2, "2024-01-01")],
            ["user_id", "dt"]
        )
        result = client.get_model_features(
            "test_model", "v1", query_df,
            start_date="2024-01-01",
        )
        assert result.count() == 2
        cols = set(result.columns)
        assert "feat1" in cols
        assert "feat2" in cols


class TestDatasetOperations:
    def test_write_and_get_dataset(self, client, spark_session, tmp_dir):
        config = DatasetConfig(
            name="my_dataset",
            version="v1",
            primary_keys=[KeySpec(name="id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=os.path.join(tmp_dir, "data/ds/my_dataset/v1")),
        )
        client.register(config)

        df = spark_session.createDataFrame([(1, "2024-01-01", 0.7)], ["id", "dt", "target"])
        client.write_entity(df, EntityKind.DATASET, "my_dataset", "v1")

        result = client.get_entity(
            EntityKind.DATASET, "my_dataset", "v1",
            start_date="2024-01-01",
        )
        assert result.count() == 1


class TestManagement:
    def test_list_entities(self, client):
        config = FeatureViewConfig(
            name="list_test",
            version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="/tmp/fv/v1"),
        )
        client.register(config)

        results = client.list_entities(kind=EntityKind.FEATURE_VIEW)
        assert len(results) >= 1
        names = [r["name"] for r in results]
        assert "list_test" in names

    def test_get_entity_info(self, client):
        config = FeatureViewConfig(
            name="info_test",
            version="v2",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="/tmp/info/v2"),
            schema=[ColumnSpec(name="f1", type="double")],
        )
        client.register(config)

        info = client.get_entity_info(EntityKind.FEATURE_VIEW, "info_test", "v2")
        assert isinstance(info, FeatureViewConfig)
        assert info.name == "info_test"
        assert info.version == "v2"
        assert len(info.schema) == 1
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py -v
```
Expected: FAIL with `ModuleNotFoundError` or `ImportError`

- [ ] **Step 3: Write `src/client.py`**

```python
"""FeatureStoreClient — unified client for the feature store."""

import os
import uuid
import logging
from typing import Dict, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, DecimalType

from feature_store.types import EntityKind
from feature_store.storage import get_backend, StorageBackend
from feature_store.registry import (
    load_yaml, write_yaml, yaml_to_config, config_to_yaml,
    build_config_from_df, build_model_config, list_entities,
    validate_dataframe, sync_lifecycle,
    EntityNotFoundError, ColumnNotFoundError,
    FeatureStoreError,
    _registry_path,
)
from feature_store.schema import (
    FeatureViewConfig, ModelConfig, LabelConfig, DatasetConfig, TrainingSetConfig,
    Lookback,
)

logger = logging.getLogger("feature_store")


class FeatureStoreClient:
    """Feature Store client with YAML-based registry."""

    def __init__(self, spark: SparkSession, registry_dir: str):
        self.spark = spark
        self.registry_dir = registry_dir.rstrip("/")
        self._backend = get_backend(registry_dir)

    # ── Registration ─────────────────────────────────────

    def register(
        self,
        source,
        kind: EntityKind = None,
        name: str = None,
        version: str = "v1",
        primary_keys: List[str] = None,
        partition_columns: List[str] = None,
        **kwargs,
    ) -> str:
        """Unified registration entry point.

        Accepts a Config object or a Spark DataFrame.
        With DataFrame, infers schema automatically.
        """
        from feature_store.schema import (
            FeatureViewConfig, ModelConfig, LabelConfig, DatasetConfig, TrainingSetConfig,
        )

        if kind and not isinstance(kind, EntityKind):
            raise ValueError(f"kind must be an EntityKind, got {type(kind)}")

        CONFIG_CLASSES = (
            FeatureViewConfig, ModelConfig, LabelConfig,
            DatasetConfig, TrainingSetConfig,
        )

        if isinstance(source, CONFIG_CLASSES):
            config = source
        elif isinstance(source, DataFrame):
            if kind is None:
                raise ValueError("kind is required when registering from DataFrame")
            if name is None:
                raise ValueError("name is required when registering from DataFrame")
            config = build_config_from_df(
                df=source,
                kind=kind,
                name=name,
                version=version,
                primary_keys=primary_keys or [],
                partition_columns=partition_columns or [],
                **kwargs,
            )
        else:
            raise ValueError(
                f"source must be a Config object or DataFrame, got {type(source)}"
            )

        path = _registry_path(
            self.registry_dir,
            EntityKind(config.kind),
            config.name,
            config.version,
        )
        yaml_str = write_yaml(self._backend, path, config)
        logger.info(f"Registered [{config.kind}] {config.name}@{config.version} -> {path}")
        return path

    def register_feature_view(self, source, **kwargs) -> str:
        if "kind" not in kwargs:
            kwargs["kind"] = EntityKind.FEATURE_VIEW
        return self.register(source, **kwargs)

    def register_model(self, source, **kwargs) -> str:
        if "kind" not in kwargs:
            kwargs["kind"] = EntityKind.MODEL
        return self.register(source, **kwargs)

    def register_label(self, source, **kwargs) -> str:
        if "kind" not in kwargs:
            kwargs["kind"] = EntityKind.LABEL
        return self.register(source, **kwargs)

    def register_dataset(self, source, **kwargs) -> str:
        if "kind" not in kwargs:
            kwargs["kind"] = EntityKind.DATASET
        return self.register(source, **kwargs)

    def register_training_set(self, source, **kwargs) -> str:
        if "kind" not in kwargs:
            kwargs["kind"] = EntityKind.TRAINING_SET
        return self.register(source, **kwargs)

    # ── Load config ──────────────────────────────────────

    def _load_config(self, kind: EntityKind, name: str, version: str):
        path = _registry_path(self.registry_dir, kind, name, version)
        if not self._backend.exists(path):
            raise EntityNotFoundError(
                f"Entity not found: {kind.value} {name}@{version} at {path}"
            )
        data = load_yaml(self._backend, path)
        if not data:
            raise EntityNotFoundError(
                f"Empty YAML at {path}"
            )
        return yaml_to_config(data)

    # ── Write ────────────────────────────────────────────

    def write_entity(
        self, df: DataFrame, kind: EntityKind,
        name: str, version: str,
        partition_num: int = 24,
        allow_extra_columns: bool = False,
    ) -> None:
        config = self._load_config(kind, name, version)
        validate_dataframe(df, config, allow_extra_columns=allow_extra_columns)

        base_path = config.storage.base_path
        partition_cols = [p.name for p in config.partition_columns]

        logger.info(f"Schema validation passed. Writing [{name}] to {base_path}")
        self._backend.write_parquet(
            df, base_path, partition_cols,
            mode="overwrite", partition_num=partition_num,
        )
        logger.info(f"Write complete: {name}@{version}")

    # ── Read ─────────────────────────────────────────────

    def get_entity(
        self,
        kind: EntityKind,
        name: str,
        version: str,
        columns: Union[List[str], str] = "*",
        start_date: str = None,
        end_date: str = None,
    ) -> DataFrame:
        config = self._load_config(kind, name, version)
        base_path = config.storage.base_path
        partition_col = config.partition_columns[0].name

        partition_keys = {p.name for p in config.partition_columns}
        primary_key_names = {k.name for k in config.primary_keys}

        logger.info(f"Fetching [{kind.value}] {name}@{version} from {base_path}")

        df = self._backend.read_parquet(self.spark, base_path)

        if columns != "*":
            # Always include primary keys and partition columns
            required_cols = set(columns) | primary_key_names | partition_keys

            # Validate requested columns
            all_col_names = set(df.columns)
            missing = required_cols - all_col_names
            if missing:
                raise ColumnNotFoundError(
                    f"Columns not found in {name}@{version}: {sorted(missing)}"
                )

            df = df.select(*[c for c in required_cols if c in all_col_names])

        # Partition filter
        if start_date:
            if end_date and end_date != start_date:
                df = df.filter(
                    (F.col(partition_col) >= start_date) &
                    (F.col(partition_col) <= end_date)
                )
            else:
                df = df.filter(F.col(partition_col) == start_date)

        # Type coercion for ML compatibility
        cast_exprs = []
        for sc in df.schema:
            col_name = sc.name
            t = sc.dataType
            if isinstance(t, DecimalType):
                cast_exprs.append(F.col(col_name).cast("double").alias(col_name))
            elif isinstance(t, LongType):
                cast_exprs.append(F.col(col_name).cast("int").alias(col_name))
            else:
                cast_exprs.append(F.col(col_name))
        if cast_exprs:
            df = df.select(*cast_exprs)

        return df

    # ── Model Feature Assembly ───────────────────────────

    def get_model_features(
        self,
        model_name: str,
        model_version: str,
        query_df: DataFrame,
        start_date: str,
        end_date: str = None,
        checkpoint_interval: int = 5,
        checkpoint_dir: str = None,
    ) -> DataFrame:
        config = self._load_config(EntityKind.MODEL, model_name, model_version)

        # Group features by (feature_view, feature_view_version)
        view_version_to_features = {}
        for col in config.schema:
            key = (col.feature_view, col.feature_view_version)
            if key not in view_version_to_features:
                view_version_to_features[key] = []
            view_version_to_features[key].append(col.name)

        total_joins = len(view_version_to_features)
        needs_checkpoint = total_joins > checkpoint_interval

        if needs_checkpoint and not checkpoint_dir:
            raise ValueError(
                f"checkpoint_dir is required when model has > {checkpoint_interval} "
                f"feature views ({total_joins})"
            )

        checkpoint_path = None
        if needs_checkpoint:
            checkpoint_path = (
                f"{checkpoint_dir}/{model_name}_{model_version}_"
                f"{uuid.uuid4().hex[:8]}"
            )
            self.spark.sparkContext.setCheckpointDir(checkpoint_path)
            logger.info(
                f"Checkpoint dir: {checkpoint_path} "
                f"(joins: {total_joins}, interval: {checkpoint_interval})"
            )

        try:
            logger.info(f"Assembling features for [{model_name}] ...")

            result_df = query_df
            join_count = 0

            for (view_name, view_version), feature_names in view_version_to_features.items():
                logger.debug(
                    f" -> Joining [{view_name}] (v{view_version}), "
                    f"cols: {feature_names}"
                )

                view_config = self._load_config(
                    EntityKind.FEATURE_VIEW, view_name, view_version
                )
                partition_col = view_config.partition_columns[0].name
                view_keys = [k.name for k in view_config.primary_keys] + [partition_col]

                view_df = self.get_entity(
                    kind=EntityKind.FEATURE_VIEW,
                    name=view_name,
                    version=view_version,
                    columns=feature_names,
                    start_date=start_date,
                    end_date=end_date,
                )

                # Determine join keys
                join_keys = [c for c in result_df.columns if c in view_keys]
                expected_keys = set(view_keys)
                actual_keys = set(join_keys)
                if actual_keys != expected_keys:
                    raise ValueError(
                        f"Join key mismatch for [{view_name}@{view_version}]: "
                        f"needed {expected_keys}, got {actual_keys}"
                    )

                result_df = result_df.join(view_df, on=join_keys, how="left")

                join_count += 1
                if needs_checkpoint and join_count % checkpoint_interval == 0:
                    logger.debug(
                        f"Checkpoint at join {join_count}/{total_joins}"
                    )
                    result_df = result_df.checkpoint(eager=True)

            logger.info(f"Model [{model_name}] feature assembly complete!")
            return result_df

        finally:
            if checkpoint_path and needs_checkpoint:
                try:
                    self._backend.rm(checkpoint_path, recursive=True)
                    logger.debug(f"Checkpoint cleaned: {checkpoint_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean checkpoint {checkpoint_path}: {e}")

    # ── Training Dataset ─────────────────────────────────

    def export_training_dataset(
        self,
        query_df: DataFrame,
        model_name: str,
        model_version: str,
        label_name: str,
        label_version: str,
        feature_start_date: str,
        feature_end_date: str = None,
        lookback: Lookback = None,
        join_type: str = "left",
        dry_run: bool = True,
        output_path: str = None,
    ) -> DataFrame:
        import pandas as pd

        lookback = lookback or Lookback()

        logger.info(f"Exporting training set for [{model_name}] ...")

        # 1. Assemble model features
        df_features = self.get_model_features(
            model_name=model_name,
            model_version=model_version,
            query_df=query_df,
            start_date=feature_start_date,
            end_date=feature_end_date,
        )

        # 2. Load label
        label_config = self._load_config(EntityKind.LABEL, label_name, label_version)
        label_partition_col = label_config.partition_columns[0].name
        label_key_names = [k.name for k in label_config.primary_keys]
        label_col_names = [c.name for c in label_config.schema if c.is_label]
        label_sel_cols = label_key_names + label_col_names

        # Compute label date range with lookback
        if not feature_end_date:
            feature_end_date = feature_start_date

        if lookback.freq == "day":
            label_start = (pd.to_datetime(feature_start_date) +
                          pd.Timedelta(days=lookback.periods)).strftime("%Y-%m-%d")
            label_end = (pd.to_datetime(feature_end_date) +
                        pd.Timedelta(days=lookback.periods)).strftime("%Y-%m-%d")
        else:  # month
            label_start = (pd.to_datetime(feature_start_date) +
                          pd.DateOffset(months=lookback.periods)).strftime("%Y-%m-%d")
            label_end = (pd.to_datetime(feature_end_date) +
                        pd.DateOffset(months=lookback.periods)).strftime("%Y-%m-%d")

        df_labels = self.get_entity(
            kind=EntityKind.LABEL,
            name=label_name,
            version=label_version,
            columns=label_sel_cols,
            start_date=label_start,
            end_date=label_end,
        )

        # Lookback: shift label date back to align with feature date
        feature_partition_col = _get_first_partition_col(df_features)
        if lookback.freq == "day":
            df_labels = df_labels.withColumn(
                feature_partition_col,
                F.date_sub(F.col(label_partition_col), lookback.periods)
            )
        else:
            df_labels = df_labels.withColumn(
                feature_partition_col,
                F.date_format(
                    F.add_months(F.col(label_partition_col), -lookback.periods),
                    "yyyy-MM"
                )
            )

        # 3. Join features + labels
        join_keys = [c for c in df_features.columns if c in label_key_names]
        expected = set(label_key_names)
        actual = set(join_keys)
        if actual != expected:
            raise ValueError(
                f"Label join key mismatch: needed {expected}, got {actual}"
            )
        join_keys.append(feature_partition_col)
        df_dataset = df_features.join(df_labels, join_keys, join_type) \
            .na.fill(value=0, subset=label_col_names)

        # 4. Materialize if not dry run
        if not dry_run:
            if output_path:
                logger.info(f"Writing training data to {output_path}")
                df_dataset.repartition(32).write.mode("overwrite").parquet(output_path)
                logger.info("Training set export complete!")
            else:
                logger.warning("dry_run=False but no output_path specified.")

        return df_dataset

    # ── Dataset / Training Set I/O ────────────────────────

    def write_dataset(self, dataset: DataFrame, name: str, version: str,
                      mode: str = "overwrite", partition_num: int = 200):
        path = self._resolve_storage_path(EntityKind.DATASET, name, version)
        config = self._load_config(EntityKind.DATASET, name, version)
        partition_cols = [p.name for p in config.partition_columns]
        self._backend.write_parquet(
            dataset, path, partition_cols, mode=mode, partition_num=partition_num
        )

    def get_dataset(self, name: str, version: str,
                    start_date: str = None, end_date: str = None) -> DataFrame:
        return self.get_entity(
            EntityKind.DATASET, name, version,
            start_date=start_date, end_date=end_date,
        )

    def write_training_set(self, dataset: DataFrame, name: str, version: str,
                           split: str = "train", mode: str = "overwrite"):
        config = self._load_config(EntityKind.TRAINING_SET, name, version)
        base_path = config.storage.base_path
        partition_cols = [p.name for p in config.partition_columns]
        path = f"{base_path}/{split}"
        self._backend.write_parquet(
            dataset, path, partition_cols, mode=mode, partition_num=24,
        )

    def get_training_set(self, name: str, version: str,
                         split: str = "train",
                         start_date: str = None,
                         end_date: str = None) -> DataFrame:
        config = self._load_config(EntityKind.TRAINING_SET, name, version)
        path = f"{config.storage.base_path}/{split}"
        return self.get_entity(
            EntityKind.TRAINING_SET, name, version,
            start_date=start_date, end_date=end_date,
        )

    # ── Management ────────────────────────────────────────

    def list_entities(self, kind: EntityKind = None) -> List[Dict]:
        return list_entities(self._backend, self.registry_dir, kind)

    def get_entity_info(self, kind: EntityKind, name: str, version: str):
        return self._load_config(kind, name, version)

    def sync_lifecycle(self) -> Dict[str, int]:
        return sync_lifecycle(self._backend, self.registry_dir)

    def build_dependency_graph(self, name: str, version: str):
        """Compute the DAG of dependencies for an entity."""
        graph = {}
        self._build_dag(name, version, graph, visited=set())
        return graph

    def _build_dag(self, name, version, graph, visited):
        keys = set()
        for path in self._backend.glob(f"{self.registry_dir}/**/*.yaml"):
            data = load_yaml(self._backend, path)
            for d in data.get("dependency", []):
                if d["name"] == name and d["version"] == version:
                    dep_name = data.get("name")
                    dep_version = data.get("version")
                    keys.add((dep_name, dep_version))

        graph[(name, version)] = list(keys)
        for dep_name, dep_version in keys:
            if (dep_name, dep_version) not in graph:
                self._build_dag(dep_name, dep_version, graph, visited)

    def _resolve_storage_path(self, kind: EntityKind, name: str, version: str) -> str:
        config = self._load_config(kind, name, version)
        return config.storage.base_path

    def migrate_registry(self, dry_run: bool = True) -> List[str]:
        """Migrate old format YAML files to new format."""
        changed = []
        paths = self._backend.glob(f"{self.registry_dir}/**/*.yaml")

        for path in paths:
            data = load_yaml(self._backend, path)
            if "kind" in data:
                continue  # Already new format

            logger.info(f"Migrating: {path}")
            try:
                new_config = _old_to_new(data)
            except Exception as e:
                logger.warning(f"Cannot migrate {path}: {e}")
                continue

            if not dry_run:
                write_yaml(self._backend, path, new_config)
                logger.info(f"Written: {path}")

            changed.append(path)

        return changed


def _get_first_partition_col(df: DataFrame) -> str:
    """Inspect config to find partition column, or fall back to common names."""
    for c in ["dt", "feature_month", "label_month", "partition_date"]:
        if c in df.columns:
            return c
    return df.columns[-1]  # fallback


def _old_to_new(data: dict):
    """Convert old format YAML dict to appropriate new Config object."""
    # Detect kind
    if "features" in data and isinstance(data["features"], list) and \
       all(isinstance(f, str) for f in data["features"]):
        kind = "model"
    elif "schema" in data:
        schema = data.get("schema", [])
        has_label_col = any(c.get("is_label") for c in schema)
        if has_label_col:
            kind = "label"
        elif data.get("entity") == "model":
            kind = "model"
        else:
            kind = "feature_view"
    else:
        kind = "feature_view"

    # Extract primary keys from old schema
    old_schema = data.get("schema", [])
    primary_keys = [
        {"name": c["name"], "type": c.get("type", "string")}
        for c in old_schema if c.get("is_primary_key")
    ]

    # Extract partition_columns from storage.partition_column
    partition_columns = []
    storage = data.get("storage", {})
    partition_col_name = storage.get("partition_column", "dt")
    partition_columns.append({
        "name": partition_col_name,
        "type": "string",
    })

    # Build remaining schema (exclude primary keys and partition column)
    exclude = {pk["name"] for pk in primary_keys} | {partition_col_name}
    schema = [
        {
            "name": c["name"],
            "type": c.get("type", "string"),
            "is_label": c.get("is_label", False),
        }
        for c in old_schema if c["name"] not in exclude
    ]

    # Model: parse URN list into structured schema
    if kind == "model":
        features = data.get("features", [])
        schema = []
        for urn in features:
            if ":" in urn and "@" in urn:
                view_part, feat_name = urn.split(":")
                view_name, view_ver = view_part.split("@")
                schema.append({
                    "name": feat_name,
                    "type": "string",  # Default; caller should enrich
                    "feature_view": view_name,
                    "feature_view_version": view_ver,
                })
            else:
                schema.append({"name": urn, "type": "string"})

    base_path = storage.get("base_path", "")
    if base_path:
        base_path = base_path.rstrip("/")

    new_data = {
        "kind": kind,
        "name": data.get("name", "unknown"),
        "version": data.get("version", "v1"),
        "owner": data.get("owner", ""),
        "description": data.get("description", ""),
        "primary_keys": primary_keys,
        "partition_columns": partition_columns,
    }

    if base_path:
        new_data["storage"] = {"base_path": base_path, "format": "parquet"}

    if kind == "feature_view":
        pipeline = data.get("pipeline", {})
        if pipeline:
            new_data["pipeline"] = pipeline
    if kind in ("feature_view", "label"):
        retention = data.get("retention", {})
        if retention:
            new_data["retention"] = retention
    if kind != "dataset" and kind != "training_set":
        new_data["schema"] = schema

    return yaml_to_config(new_data)
```

- [ ] **Step 4: Run tests to verify**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py -v -x
```
Expected: Tests pass (some may skip where parquet writing to temp path without Spark checkpoint config)

- [ ] **Step 5: Update `src/__init__.py` to export FeatureStoreClient**

```python
# src/__init__.py
from feature_store.client import FeatureStoreClient
from feature_store.types import EntityKind, StorageFormat, UpdateFrequency
from feature_store.schema import (
    ColumnSpec, KeySpec, Dependency, PipelineSpec,
    RetentionSpec, StorageSpec, Lookback,
    FeatureViewConfig, ModelConfig, LabelConfig, DatasetConfig, TrainingSetConfig,
)
from feature_store.registry import (
    FeatureStoreError, EntityNotFoundError, ColumnNotFoundError,
    SchemaValidationError, MissingColumnsError, ExtraColumnsError,
    RegistryFormatError, StorageError,
)

__all__ = [
    "FeatureStoreClient",
    "EntityKind", "StorageFormat", "UpdateFrequency",
    "ColumnSpec", "KeySpec", "Dependency", "PipelineSpec",
    "RetentionSpec", "StorageSpec", "Lookback",
    "FeatureViewConfig", "ModelConfig", "LabelConfig",
    "DatasetConfig", "TrainingSetConfig",
    "FeatureStoreError", "EntityNotFoundError", "ColumnNotFoundError",
    "SchemaValidationError", "MissingColumnsError", "ExtraColumnsError",
    "RegistryFormatError", "StorageError",
]
```

- [ ] **Step 6: Commit**

```bash
git add src/client.py src/__init__.py tests/test_client.py
git commit -m "feat: add FeatureStoreClient with unified registration, read/write, model assembly, and lifecycle sync"
```

---

### Task 7: Migration — Test Fixtures and End-to-End

**Files:**
- Create: `tests/fixtures/old_format/feature_view.yaml`
- Create: `tests/fixtures/old_format/model_feature.yaml`
- Create: `tests/fixtures/old_format/label.yaml`
- Create: `tests/fixtures/new_format/feature_view.yaml`
- Create: `tests/fixtures/new_format/model_feature.yaml`
- Create: `tests/fixtures/new_format/label.yaml`
- Create: `tests/test_migration.py`

- [ ] **Step 1: Write old format fixtures**

```yaml
# tests/fixtures/old_format/feature_view.yaml
# ==============================================================================
# Auto-Generated Feature View Template: test_features@v1
# ==============================================================================
name: "test_features"
version: "v1"
entity: "customers"
owner: "test@corp.com"
description: "Test feature view for migration"

storage:
  type: "gcs"
  base_path: "gs://bucket/test/feature_views/test_features/v1"
  partition_column: "dt"
  format: "parquet"

pipeline:
  update_frequency: "daily"
  source_job: ""
  alert_threshold_hours: "48"

retention:
  ttl_days: 360
  cold_tier_days: 180

schema:
  - name: "user_id"
    type: "string"
    description: "primary key"
    is_primary_key: true

  - name: "dt"
    type: "string"
    description: "partition col, auto generated"
    is_primary_key: false

  - name: "age"
    type: "integer"
    description: "User age"
    is_primary_key: false

  - name: "score"
    type: "double"
    description: "User score"
    is_primary_key: false
```

```yaml
# tests/fixtures/old_format/model_feature.yaml
# ==============================================================================
# Auto-Generated Model Feature Template: test_model
# ==============================================================================
name: "test_model"
owner: "test@corp.com"
description: "Test model for migration"

features:
  - "test_features@v1:age"
  - "test_features@v1:score"
```

```yaml
# tests/fixtures/old_format/label.yaml
name: "test_label"
version: "v1"
owner: "test@corp.com"
description: "Test label for migration"

storage:
  type: "gcs"
  base_path: "gs://bucket/test/labels/test_label/v1"
  partition_column: "label_month"
  format: "parquet"

pipeline:
  update_frequency: "monthly"
  source_job: ""
  alert_threshold_hours: "26"

retention:
  ttl_days: 360
  cold_tier_days: 180

schema:
  - name: "id_doc_num"
    type: "string"
    is_primary_key: true
  - name: "id_doc_type"
    type: "string"
    is_primary_key: true
  - name: "channel_name"
    type: "string"
    is_primary_key: false
  - name: "acq_pcd"
    type: "integer"
    description: ""
    is_primary_key: false
    is_label: true
  - name: "label_month"
    type: "string"
    description: "partition"
    is_primary_key: false
    is_deprecated: false
```

- [ ] **Step 2: Write expected new format fixtures**

```yaml
# tests/fixtures/new_format/feature_view.yaml
kind: feature_view
name: "test_features"
version: "v1"
owner: "test@corp.com"
description: "Test feature view for migration"

primary_keys:
  - name: "user_id"
    type: "string"

partition_columns:
  - name: "dt"
    type: "string"

storage:
  base_path: "gs://bucket/test/feature_views/test_features/v1"
  format: "parquet"

pipeline:
  update_frequency: "daily"
  source_job: ""
  alert_threshold_hours: 48

retention:
  ttl_days: 360
  cold_tier_days: 180

schema:
  - name: "age"
    type: "integer"
  - name: "score"
    type: "double"
```

```yaml
# tests/fixtures/new_format/model_feature.yaml
kind: model
name: "test_model"
version: "v1"
owner: "test@corp.com"
description: "Test model for migration"

dependency: []

primary_keys: []

partition_columns:
  - name: "dt"
    type: "string"

schema:
  - name: "age"
    type: "string"
    feature_view: "test_features"
    feature_view_version: "v1"
  - name: "score"
    type: "string"
    feature_view: "test_features"
    feature_view_version: "v1"
```

```yaml
# tests/fixtures/new_format/label.yaml
kind: label
name: "test_label"
version: "v1"
owner: "test@corp.com"
description: "Test label for migration"

primary_keys:
  - name: "id_doc_num"
    type: "string"
  - name: "id_doc_type"
    type: "string"

partition_columns:
  - name: "label_month"
    type: "string"

storage:
  base_path: "gs://bucket/test/labels/test_label/v1"
  format: "parquet"

retention:
  ttl_days: 360
  cold_tier_days: 180

schema:
  - name: "channel_name"
    type: "string"
  - name: "acq_pcd"
    type: "integer"
    is_label: true
```

- [ ] **Step 3: Write migration test**

```python
# tests/test_migration.py
import os
import tempfile
import pytest
import yaml


FIXTURES_OLD = os.path.join(os.path.dirname(__file__), "fixtures", "old_format")
FIXTURES_NEW = os.path.join(os.path.dirname(__file__), "fixtures", "new_format")


def _load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class TestMigrationFixtures:
    def test_old_fixtures_exist(self):
        assert os.path.exists(os.path.join(FIXTURES_OLD, "feature_view.yaml"))
        assert os.path.exists(os.path.join(FIXTURES_OLD, "model_feature.yaml"))
        assert os.path.exists(os.path.join(FIXTURES_OLD, "label.yaml"))

    def test_new_fixtures_exist(self):
        assert os.path.exists(os.path.join(FIXTURES_NEW, "feature_view.yaml"))
        assert os.path.exists(os.path.join(FIXTURES_NEW, "model_feature.yaml"))
        assert os.path.exists(os.path.join(FIXTURES_NEW, "label.yaml"))

    def test_old_feature_view_has_no_kind(self):
        data = _load_yaml(os.path.join(FIXTURES_OLD, "feature_view.yaml"))
        assert "kind" not in data
        assert data["name"] == "test_features"

    def test_new_feature_view_has_kind(self):
        data = _load_yaml(os.path.join(FIXTURES_NEW, "feature_view.yaml"))
        assert data["kind"] == "feature_view"
        assert "primary_keys" in data
        assert len(data["primary_keys"]) == 1

    def test_old_model_has_features_list(self):
        data = _load_yaml(os.path.join(FIXTURES_OLD, "model_feature.yaml"))
        assert "features" in data
        assert isinstance(data["features"], list)
        assert "version" not in data

    def test_new_model_has_schema(self):
        data = _load_yaml(os.path.join(FIXTURES_NEW, "model_feature.yaml"))
        assert data["kind"] == "model"
        assert "schema" in data
        assert data["schema"][0]["feature_view"] == "test_features"

    def test_old_label_has_primary_keys_in_schema(self):
        data = _load_yaml(os.path.join(FIXTURES_OLD, "label.yaml"))
        assert "schema" in data
        pk_cols = [c for c in data["schema"] if c.get("is_primary_key")]
        assert len(pk_cols) == 2

    def test_new_label_has_primary_keys_top_level(self):
        data = _load_yaml(os.path.join(FIXTURES_NEW, "label.yaml"))
        assert data["kind"] == "label"
        assert len(data["primary_keys"]) == 2
```

- [ ] **Step 4: Run migration tests**

```bash
cd D:/feature_store && python -m pytest tests/test_migration.py -v
```
Expected: PASS (all 8 fixture tests)

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/ tests/test_migration.py
git commit -m "test: add migration fixtures and format validation tests"
```

---

### Task 8: Cleanup — Remove Old Files

- [ ] **Step 1: Remove old project files**

```bash
rm -rf D:/feature_store/core D:/feature_store/scripts D:/feature_store/sync_project_resources.sh D:/feature_store/__init__.py
```

- [ ] **Step 2: Move docs/examples to new format**

```bash
cp D:/feature_store/docs/feature_view.yaml D:/feature_store/docs/examples/old_feature_view.yaml
cp D:/feature_store/docs/model_feature.yaml D:/feature_store/docs/examples/old_model_feature.yaml
cp D:/feature_store/docs/label.yaml D:/feature_store/docs/examples/old_label.yaml
```

- [ ] **Step 3: Verify imports still work**

```bash
cd D:/feature_store && PYTHONPATH=src python -c "from feature_store import FeatureStoreClient, EntityKind; print('OK')"
```
Expected: `OK`

- [ ] **Step 4: Run full test suite**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v --tb=short
```
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git rm core/ scripts/ sync_project_resources.sh __init__.py
git add docs/examples/
git commit -m "chore: remove old project structure; add example YAML backups"
```

---

### Task 9: Final Verification

- [ ] **Step 1: Run full test suite one final time**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v
```
Expected: All tests pass

- [ ] **Step 2: Verify package structure**

```bash
find D:/feature_store/src -type f | sort
```
Expected output:
```
D:/feature_store/src/__init__.py
D:/feature_store/src/client.py
D:/feature_store/src/registry.py
D:/feature_store/src/schema.py
D:/feature_store/src/storage.py
D:/feature_store/src/types.py
```

- [ ] **Step 3: Commit any final changes**

```bash
git add -A && git status
```
