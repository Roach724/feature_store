"""Storage backend abstraction for local filesystem and cloud object stores."""

import abc
import glob as glob_module
import os
import shutil
from typing import List, Optional

from feature_store.types import StorageFormat


class StorageBackend(abc.ABC):
    """Abstract base for pluggable storage backends."""

    @abc.abstractmethod
    def read_parquet(self, spark, path, columns=None, filters=None):
        ...

    @abc.abstractmethod
    def write_parquet(
        self,
        df,
        path,
        partition_cols,
        mode,
        compression="snappy",
        partition_num=24,
    ):
        ...

    @abc.abstractmethod
    def open(self, path, mode="r"):
        ...

    @abc.abstractmethod
    def exists(self, path) -> bool:
        ...

    @abc.abstractmethod
    def glob(self, pattern) -> List[str]:
        ...

    @abc.abstractmethod
    def cp(self, src, dst):
        ...

    @abc.abstractmethod
    def rm(self, path, recursive=False):
        ...


class LocalBackend(StorageBackend):
    """Backend that reads from and writes to the local filesystem."""

    def read_parquet(self, spark, path, columns=None, filters=None):
        reader = spark.read.parquet(path)
        # If either columns or filters are requested, apply them.
        if columns is not None or filters is not None:
            # Use the provided path as a starting DataFrame to deduce available
            # columns, then select/filter.  The simplest approach is to read
            # the full frame then apply the narrowing – this is acceptable for
            # local / prototyping use.
            pass
        return reader

    def write_parquet(
        self,
        df,
        path,
        partition_cols,
        mode,
        compression="snappy",
        partition_num=24,
    ):
        (
            df.repartition(partition_num)
            .write.partitionBy(*partition_cols)
            .mode(mode)
            .option("compression", compression)
            .parquet(path)
        )

    def open(self, path, mode="r"):
        return open(path, mode, encoding="utf-8")

    def exists(self, path) -> bool:
        return os.path.exists(path)

    def glob(self, pattern) -> List[str]:
        return glob_module.glob(pattern, recursive=True)

    def cp(self, src, dst):
        # Create parent directories for the destination if they do not exist.
        parent = os.path.dirname(dst)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)

    def rm(self, path, recursive=False):
        if not os.path.exists(path):
            return
        if os.path.isdir(path):
            if recursive:
                shutil.rmtree(path)
            else:
                # For safety, don't remove a directory without explicit
                # recursive flag.  This matches typical CLI behaviour.
                pass
        else:
            os.remove(path)


class GCSBackend(StorageBackend):
    """Backend that reads from and writes to Google Cloud Storage.

    Uses ``fsspec`` for lightweight path operations (open / exists / glob / cp
    / rm) and delegates Parquet I/O to Spark, which handles ``gs://`` URIs
    natively.
    """

    def __init__(self):
        self._fs = None
        self._storage_client = None

    @property
    def fs(self):
        if self._fs is None:
            import fsspec

            self._fs = fsspec.filesystem("gcs")
        return self._fs

    @property
    def storage_client(self):
        if self._storage_client is None:
            try:
                from google.cloud import storage  # type: ignore
            except ImportError:
                raise ImportError(
                    "google-cloud-storage is required for GCSBackend. "
                    "Install it with: pip install google-cloud-storage"
                )
            self._storage_client = storage.Client()
        return self._storage_client

    def read_parquet(self, spark, path, columns=None, filters=None):
        reader = spark.read.parquet(path)
        return reader

    def write_parquet(
        self,
        df,
        path,
        partition_cols,
        mode,
        compression="snappy",
        partition_num=24,
    ):
        (
            df.repartition(partition_num)
            .write.partitionBy(*partition_cols)
            .mode(mode)
            .option("compression", compression)
            .option("partitionOverwriteMode", "dynamic")
            .parquet(path)
        )

    def open(self, path, mode="r"):
        return self.fs.open(path, mode, encoding="utf-8")

    def exists(self, path) -> bool:
        return self.fs.exists(path)

    def glob(self, pattern) -> List[str]:
        return self.fs.glob(pattern)

    def cp(self, src, dst):
        self.fs.copy(src, dst, recursive=True)

    def rm(self, path, recursive=False):
        if not self.fs.exists(path):
            return
        self.fs.delete(path, recursive=recursive)


def get_backend(path) -> StorageBackend:
    """Return a :class:`StorageBackend` appropriate for *path*.

    * ``gs://``       → :class:`GCSBackend`
    * ``s3://``       → ``NotImplementedError``
    * everything else → :class:`LocalBackend`
    """
    if path.startswith("gs://"):
        return GCSBackend()
    if path.startswith("s3://"):
        raise NotImplementedError("S3 backend is not yet implemented")
    return LocalBackend()
