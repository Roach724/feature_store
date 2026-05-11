# Module: `feature_store.storage`

Pluggable storage backend abstraction. Provides a common interface for reading
and writing Parquet data, plus filesystem operations (open, exists, glob, copy,
remove). Two implementations are included: local filesystem and Google Cloud
Storage.

**Source:** `src/feature_store/storage.py`

---

## Public Exports

| Symbol | Type | Description |
|--------|------|-------------|
| `StorageBackend` | ABC | Abstract interface for storage operations |
| `LocalBackend` | Class | Local filesystem using pyarrow/pandas |
| `GCSBackend` | Class | GCS using Spark native Parquet + fsspec |
| `get_backend` | Function | Factory: selects backend based on path scheme |

---

## Detailed API

### `StorageBackend` (ABC)

```python
class StorageBackend(abc.ABC):
    def read_parquet(self, spark, path, columns=None, filters=None): ...
    def write_parquet(self, df, path, partition_cols, mode, compression="snappy", partition_num=24): ...
    def open(self, path, mode="r"): ...
    def exists(self, path) -> bool: ...
    def glob(self, pattern) -> List[str]: ...
    def cp(self, src, dst): ...
    def rm(self, path, recursive=False): ...
```

Abstract base class defining the storage interface. All backend implementations
must implement these seven methods.

| Method | Description |
|--------|-------------|
| `read_parquet(spark, path, columns, filters)` | Read a Parquet dataset into a Spark DataFrame |
| `write_parquet(df, path, partition_cols, mode, compression, partition_num)` | Write a DataFrame as Parquet |
| `open(path, mode)` | Open a file for reading/writing text |
| `exists(path)` | Check whether a path exists |
| `glob(pattern)` | List paths matching a glob pattern |
| `cp(src, dst)` | Copy a file or directory |
| `rm(path, recursive)` | Remove a file or directory |

---

### `LocalBackend`

```python
class LocalBackend(StorageBackend):
    ...
```

Backend that reads from and writes to the local filesystem. Uses pyarrow/pandas
for Parquet I/O to avoid Hadoop filesystem issues on Windows.

**Parquet I/O strategy:**
- **Read:** Uses `pyarrow.parquet.read_table` → pandas → Spark DataFrame
- **Write:** Uses `pyarrow.parquet.write_to_dataset` (partitioned) or
  `write_table` (unpartitioned)
- **Modes:** `"overwrite"` (rmtree + write), `"append"` (write alongside),
  `"ignore"` (skip if exists)

**Usage:**

```python
from feature_store import LocalBackend

backend = LocalBackend()
backend.write_parquet(df, "/data/features", partition_cols=["dt"], mode="overwrite")
df = backend.read_parquet(spark, "/data/features", columns=["age", "score"])
```

---

### `GCSBackend`

```python
class GCSBackend(StorageBackend):
    ...
```

Backend that reads from and writes to Google Cloud Storage (`gs://` paths).

**Dependencies:**
- Parquet I/O: Spark native GCS connector (handles `gs://` URIs natively)
- File operations: `fsspec` with `gcsfs` for open, exists, glob, cp, rm
- Lifecycle: `google-cloud-storage` for bucket lifecycle management

**Parquet I/O strategy:**
- **Read:** `spark.read.parquet(path)` — Spark native GCS
- **Write:** `df.write.partitionBy(...).mode(mode).parquet(path)` —
  includes `partitionOverwriteMode=dynamic` for safe partition updates
- **Repartition:** Data is repartitioned to `partition_num` before write

**Usage:**

```python
from feature_store import GCSBackend

backend = GCSBackend()
backend.write_parquet(
    df, "gs://bucket/features/user/v1",
    partition_cols=["dt"], mode="overwrite", partition_num=24,
)
```

---

### `get_backend`

```python
def get_backend(path: str) -> StorageBackend:
```

Factory function that returns the appropriate `StorageBackend` for a given path.

| Path pattern | Backend |
|-------------|---------|
| `gs://...` | `GCSBackend` |
| `s3://...` | raises `NotImplementedError` |
| everything else | `LocalBackend` |

**Usage:**

```python
from feature_store import get_backend

backend = get_backend("gs://bucket/registry")   # → GCSBackend
backend = get_backend("/local/registry")        # → LocalBackend
```

The `FeatureStoreClient` constructor calls this automatically based on
`registry_dir`.

---

## Cross-References

- [Client Module](api_client.md) — uses `get_backend` for constructor
- [Registry Module](api_registry.md) — `load_yaml`/`write_yaml` use backend I/O
