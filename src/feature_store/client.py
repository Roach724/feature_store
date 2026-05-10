"""FeatureStoreClient — unified interface for feature store operations."""

import logging
import os
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import is_dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from feature_store.types import EntityKind
from feature_store.schema import (
    FeatureViewConfig,
    ModelConfig,
    LabelConfig,
    DatasetConfig,
    TrainingSetConfig,
    Lookback,
)
from feature_store.storage import get_backend
from feature_store.registry import (
    load_yaml,
    write_yaml,
    yaml_to_config,
    build_config_from_df,
    validate_dataframe,
    list_entities,
    sync_lifecycle as _sync_lifecycle,
    EntityNotFoundError,
    _registry_path,
)

logger = logging.getLogger("feature_store")

_CONFIG_CLASSES = (FeatureViewConfig, ModelConfig, LabelConfig, DatasetConfig)


# ---------------------------------------------------------------------------
# Migration helper
# ---------------------------------------------------------------------------

def _old_to_new(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert legacy registry YAML data to the current schema.

    Adds a ``kind`` key if missing and normalises known fields.
    """
    if "kind" not in data:
        # Heuristic inference of the entity kind.
        schema = data.get("schema", [])
        if any(col.get("is_label") for col in schema):
            data["kind"] = "label"
        elif any(col.get("feature_view") for col in schema):
            data["kind"] = "model"
        elif "pipeline" in data:
            data["kind"] = "feature_view"
        elif "storage" in data:
            data["kind"] = "dataset"
        else:
            data["kind"] = "feature_view"  # safest default

    # Ensure lists are present for required fields so yaml_to_config never
    # gets ``None`` for what it expects to be a list.
    for list_key in ("primary_keys", "partition_columns", "schema", "dependency"):
        if data.get(list_key) is None:
            data[list_key] = []

    # Normalise version to a string.
    if "version" in data and not isinstance(data["version"], str):
        data["version"] = str(data["version"])

    return data


# ---------------------------------------------------------------------------
# CheckpointContext
# ---------------------------------------------------------------------------

class CheckpointContext:
    """Holds a checkpoint directory path.

    The owning context manager is responsible for calling :meth:`cleanup`.
    """

    def __init__(self, backend, path: str):
        self.backend = backend
        self.path = path

    def cleanup(self):
        if self.path and self.backend.exists(self.path):
            self.backend.rm(self.path, recursive=True)
        self.path = None


# ---------------------------------------------------------------------------
# FeatureStoreClient
# ---------------------------------------------------------------------------

class FeatureStoreClient:
    """High-level client for registering, reading, and writing feature store
    entities backed by a local or cloud storage backend."""

    def __init__(self, spark, registry_dir: str):
        self.spark = spark
        self.registry_dir = registry_dir.rstrip("/").rstrip("\\").replace("\\", "/")
        self.backend = get_backend(self.registry_dir)

    # -----------------------------------------------------------------------
    # Checkpoint context manager
    # -----------------------------------------------------------------------

    @contextmanager
    def checkpoint_context(self, base_dir: str):
        """Context manager that creates a unique checkpoint directory, sets it
        on the SparkContext, and guarantees cleanup on exit."""
        path = f"{base_dir}/{uuid.uuid4().hex[:8]}"
        self.spark.sparkContext.setCheckpointDir(path)
        ctx = CheckpointContext(self.backend, path)
        logger.info("Checkpoint context created: %s", path)
        try:
            yield ctx
        finally:
            ctx.cleanup()
            logger.debug("Checkpoint context cleaned: %s", path)

    # -----------------------------------------------------------------------
    # Registration
    # -----------------------------------------------------------------------

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
        """Register an entity from a config dataclass or a Spark DataFrame.

        Returns the path to the written YAML registry file.
        """
        if isinstance(source, _CONFIG_CLASSES):
            config = source
        else:
            # DataFrame path — infer config from its schema.
            config = build_config_from_df(
                df=source,
                kind=kind,
                name=name,
                version=version,
                primary_keys=primary_keys,
                partition_columns=partition_columns,
                **kwargs,
            )

        entity_kind = EntityKind(config.kind)
        path = _registry_path(
            self.registry_dir, entity_kind, config.name, config.version
        )
        write_yaml(self.backend, path, config)
        logger.info(
            "Registered %s/%s/%s → %s",
            config.kind, config.name, config.version, path,
        )
        return path

    # Convenience registration methods -----------------------------------

    def register_feature_view(
        self,
        source,
        name: Optional[str] = None,
        version: str = "v1",
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> str:
        return self.register(
            source=source,
            kind=EntityKind.FEATURE_VIEW,
            name=name,
            version=version,
            primary_keys=primary_keys,
            partition_columns=partition_columns,
            **kwargs,
        )

    def register_model(
        self,
        source,
        name: Optional[str] = None,
        version: str = "v1",
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> str:
        return self.register(
            source=source,
            kind=EntityKind.MODEL,
            name=name,
            version=version,
            primary_keys=primary_keys,
            partition_columns=partition_columns,
            **kwargs,
        )

    def register_label(
        self,
        source,
        name: Optional[str] = None,
        version: str = "v1",
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> str:
        return self.register(
            source=source,
            kind=EntityKind.LABEL,
            name=name,
            version=version,
            primary_keys=primary_keys,
            partition_columns=partition_columns,
            **kwargs,
        )

    def register_dataset(
        self,
        source,
        name: Optional[str] = None,
        version: str = "v1",
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> str:
        return self.register(
            source=source,
            kind=EntityKind.DATASET,
            name=name,
            version=version,
            primary_keys=primary_keys,
            partition_columns=partition_columns,
            **kwargs,
        )

    def register_training_set(
        self,
        source,
        name: Optional[str] = None,
        version: str = "v1",
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> str:
        return self.register(
            source=source,
            kind=EntityKind.TRAINING_SET,
            name=name,
            version=version,
            primary_keys=primary_keys,
            partition_columns=partition_columns,
            **kwargs,
        )

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _load_config(self, kind: EntityKind, name: str, version: str):
        """Load a single entity config from the registry, raising
        :class:`EntityNotFoundError` when it does not exist."""
        path = _registry_path(self.registry_dir, kind, name, version)
        if not self.backend.exists(path):
            raise EntityNotFoundError(
                f"Entity {kind.value}/{name}/{version} not found at {path}"
            )
        data = load_yaml(self.backend, path)
        return yaml_to_config(data)

    # -----------------------------------------------------------------------
    # Read / write entities
    # -----------------------------------------------------------------------

    def write_entity(
        self,
        df,
        kind: EntityKind,
        name: str,
        version: str,
        partition_num: int = 24,
        allow_extra_columns: bool = False,
    ) -> None:
        """Validate *df* against the registered config and persist it."""
        config = self._load_config(kind, name, version)
        # For configs without a schema (e.g. DatasetConfig), allow extra columns.
        if not getattr(config, "schema", None):
            allow_extra_columns = True
        validate_dataframe(df, config, allow_extra_columns=allow_extra_columns)
        base_path = config.storage.base_path.replace("\\", "/")
        partition_cols = [k.name for k in config.partition_columns]
        self.backend.write_parquet(
            df,
            base_path,
            partition_cols=partition_cols,
            mode="overwrite",
            partition_num=partition_num,
        )
        logger.info(
            "Wrote %s/%s/%s → %s", kind.value, name, version, base_path,
        )

    def get_entity(
        self,
        kind: EntityKind,
        name: str,
        version: str,
        columns="*",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """Read an entity from storage with optional column projection,
        date filtering, and type coercion."""
        config = self._load_config(kind, name, version)
        base_path = config.storage.base_path.replace("\\", "/")

        df = self.backend.read_parquet(self.spark, base_path)

        # Column projection — always include primary keys and partition columns.
        if columns != "*":
            pk_names = {k.name for k in config.primary_keys}
            pc_names = {k.name for k in config.partition_columns}
            select_cols = sorted(pk_names | pc_names | set(columns))
            df = df.select(*select_cols)

        # Partition-based date filtering.
        if start_date is not None or end_date is not None:
            pc_name = config.partition_columns[0].name
            if start_date is not None:
                df = df.filter(df[pc_name] >= start_date)
            if end_date is not None:
                df = df.filter(df[pc_name] <= end_date)

        # Type coercion: Decimal → double, Long → int.
        from pyspark.sql.types import DecimalType, LongType, DoubleType, IntegerType

        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                df = df.withColumn(field.name, df[field.name].cast(DoubleType()))
            elif isinstance(field.dataType, LongType):
                df = df.withColumn(field.name, df[field.name].cast(IntegerType()))

        return df

    # -----------------------------------------------------------------------
    # Model feature assembly
    # -----------------------------------------------------------------------

    def get_model_features(
        self,
        model_name: str,
        model_version: str,
        query_df,
        start_date: str,
        end_date: Optional[str] = None,
        checkpoint_interval: int = 5,
        checkpoint_dir: Optional[str] = None,
        checkpoint_ctx: Optional["CheckpointContext"] = None,
    ):
        """Assemble a feature DataFrame by joining columns from the feature
        views that back a registered model.

        Parameters
        ----------
        checkpoint_interval :
            When the number of feature-view joins exceeds this value, Spark
            ``checkpoint`` is called every *checkpoint_interval* joins to
            break the lineage and keep the query plan manageable.
        checkpoint_dir :
            Directory used for Spark checkpoint files.  Required when the
            number of joins exceeds *checkpoint_interval*.
        checkpoint_ctx :
            Optional :class:`CheckpointContext` whose owning context manager
            controls cleanup.  When provided, *checkpoint_dir* is ignored.
        """
        config = self._load_config(EntityKind.MODEL, model_name, model_version)

        # Group columns by their source feature view.
        view_groups: Dict[Tuple[str, str], List[str]] = defaultdict(list)
        for col in config.schema:
            view_groups[(col.feature_view, col.feature_view_version)].append(col.name)

        total_joins = len(view_groups)

        checkpoint_path = None
        owns_checkpoint = False

        if checkpoint_ctx is not None:
            if checkpoint_dir is not None:
                logger.warning(
                    "checkpoint_dir=%r ignored because checkpoint_ctx was provided",
                    checkpoint_dir,
                )
            if total_joins > checkpoint_interval:
                checkpoint_path = checkpoint_ctx.path
                self.spark.sparkContext.setCheckpointDir(checkpoint_path)
        elif total_joins > checkpoint_interval:
            if checkpoint_dir is None:
                raise ValueError(
                    f"Number of joins ({total_joins}) exceeds checkpoint_interval "
                    f"({checkpoint_interval}) but no checkpoint_dir or checkpoint_ctx was provided"
                )
            checkpoint_path = os.path.join(
                checkpoint_dir,
                f"{model_name}_{model_version}_{uuid.uuid4().hex[:8]}",
            )
            self.spark.sparkContext.setCheckpointDir(checkpoint_path)
            owns_checkpoint = True

        join_keys = [k.name for k in config.primary_keys] + [
            k.name for k in config.partition_columns
        ]

        try:
            result_df = query_df
            join_count = 0

            for (fv_name, fv_version), fv_columns in view_groups.items():
                fv_data = self.get_entity(
                    EntityKind.FEATURE_VIEW,
                    fv_name,
                    fv_version,
                    columns=fv_columns,
                    start_date=start_date,
                    end_date=end_date,
                )
                result_df = result_df.join(fv_data, on=join_keys, how="left")
                join_count += 1

                if checkpoint_path and join_count % checkpoint_interval == 0:
                    result_df = result_df.checkpoint(eager=True)

            return result_df
        finally:
            if owns_checkpoint and checkpoint_path:
                self.backend.rm(checkpoint_path, recursive=True)
                logger.debug("Checkpoint cleaned: %s", checkpoint_path)

    # -----------------------------------------------------------------------
    # Training dataset export
    # -----------------------------------------------------------------------

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
    ):
        """Join features (from a registered model) with labels and
        optionally persist the result.

        *lookback* controls how far back feature dates are shifted relative to
        label dates (default: 1 day).
        """
        if lookback is None:
            lookback = Lookback(freq="day", periods=1)

        # Compute the label date range by shifting feature dates forward.
        from pandas import DateOffset

        if lookback.freq == "day":
            offset = DateOffset(days=lookback.periods)
        else:
            offset = DateOffset(months=lookback.periods)

        label_start = feature_start_date
        label_end = feature_end_date

        # Load labels.
        label_df = self.get_entity(
            EntityKind.LABEL, label_name, label_version,
            start_date=label_start, end_date=label_end,
        )

        # Shift the label partition column *back* so it aligns with the
        # feature partition column for the join.
        label_config = self._load_config(EntityKind.LABEL, label_name, label_version)
        pc_name = label_config.partition_columns[0].name

        from pyspark.sql import functions as F

        if lookback.freq == "day":
            # Use to_date then date_sub for day-level shift.
            label_df = label_df.withColumn(
                pc_name,
                F.date_sub(F.to_date(F.col(pc_name)), lookback.periods),
            )
        else:
            # Use to_date then add_months for month-level shift.
            label_df = label_df.withColumn(
                pc_name,
                F.add_months(F.to_date(F.col(pc_name)), -lookback.periods),
            )
        # Convert back to string to match feature partition column type.
        label_df = label_df.withColumn(pc_name, F.date_format(F.col(pc_name), "yyyy-MM-dd"))

        # Get features from the model.
        feature_df = self.get_model_features(
            model_name, model_version, query_df,
            start_date=feature_start_date, end_date=feature_end_date,
            checkpoint_ctx=checkpoint_ctx,
        )

        # Join features with labels.
        join_keys = [k.name for k in label_config.primary_keys] + [pc_name]
        dataset_df = feature_df.join(label_df, on=join_keys, how=join_type)

        if not dry_run and output_path is not None:
            partition_cols = [k.name for k in label_config.partition_columns]
            self.backend.write_parquet(
                dataset_df, output_path,
                partition_cols=partition_cols,
                mode="overwrite",
            )

        return dataset_df

    # -----------------------------------------------------------------------
    # Dataset operations
    # -----------------------------------------------------------------------

    def write_dataset(
        self,
        dataset,
        name: str,
        version: str,
        mode: str = "overwrite",
        partition_num: int = 200,
    ) -> None:
        """Write a dataset entity."""
        self.write_entity(
            dataset, EntityKind.DATASET, name, version,
            partition_num=partition_num,
        )

    def get_dataset(
        self,
        name: str,
        version: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """Read a dataset entity."""
        return self.get_entity(
            EntityKind.DATASET, name, version,
            start_date=start_date, end_date=end_date,
        )

    # -----------------------------------------------------------------------
    # Training set operations
    # -----------------------------------------------------------------------

    def write_training_set(
        self,
        dataset,
        name: str,
        version: str,
        split: str = "train",
        mode: str = "overwrite",
    ) -> None:
        """Write a training / validation / test split."""
        config = self._load_config(EntityKind.TRAINING_SET, name, version)
        base_path = (config.storage.base_path.replace("\\", "/") + "/" + split)
        partition_cols = [k.name for k in config.partition_columns]
        self.backend.write_parquet(
            dataset,
            base_path,
            partition_cols=partition_cols,
            mode=mode,
        )

    def get_training_set(
        self,
        name: str,
        version: str,
        split: str = "train",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        """Read a training / validation / test split."""
        config = self._load_config(EntityKind.TRAINING_SET, name, version)
        base_path = (config.storage.base_path.replace("\\", "/") + "/" + split)

        df = self.backend.read_parquet(self.spark, base_path)

        # Apply date filtering.
        if start_date is not None or end_date is not None:
            pc_name = config.partition_columns[0].name
            if start_date is not None:
                df = df.filter(df[pc_name] >= start_date)
            if end_date is not None:
                df = df.filter(df[pc_name] <= end_date)

        return df

    # -----------------------------------------------------------------------
    # Management
    # -----------------------------------------------------------------------

    def list_entities(self, kind: Optional[EntityKind] = None) -> List[Dict]:
        """Return a list of dicts with a ``"name"`` key for each registered
        entity of *kind* (or all kinds when *kind* is ``None``)."""
        entities = list_entities(self.backend, self.registry_dir, kind=kind)
        # Flatten: return the data dict (which already contains "name").
        return [e["data"] for e in entities]

    def get_entity_info(self, kind: EntityKind, name: str, version: str):
        """Return the typed config object for a registered entity."""
        return self._load_config(kind, name, version)

    def sync_lifecycle(self) -> Dict[str, int]:
        """Scan registry YAMLs and apply GCS lifecycle rules.

        Returns a dict mapping ``"name/version"`` to status codes
        (``1`` = success, ``-1`` = failure).
        """
        return _sync_lifecycle(self.backend, self.registry_dir)

    def build_dependency_graph(
        self, name: str, version: str
    ) -> Dict[str, List[str]]:
        """Perform a recursive BFS over entity dependencies and return an
        adjacency dict mapping ``"name/version"`` → ``["dep1/v1", ...]``."""
        from collections import deque

        graph: Dict[str, List[str]] = {}
        visited: set = set()
        queue: deque = deque()
        queue.append((name, version))

        while queue:
            cur_name, cur_version = queue.popleft()
            key = f"{cur_name}/{cur_version}"
            if key in visited:
                continue
            visited.add(key)

            config = None
            for k in EntityKind:
                try:
                    config = self._load_config(k, cur_name, cur_version)
                    break
                except EntityNotFoundError:
                    continue

            if config is None:
                graph[key] = []
                continue

            deps = [f"{d.name}/{d.version}" for d in config.dependency]
            graph[key] = deps
            for dep in config.dependency:
                if f"{dep.name}/{dep.version}" not in visited:
                    queue.append((dep.name, dep.version))

        return graph

    def migrate_registry(self, dry_run: bool = True) -> List[str]:
        """Scan all registry YAMLs, apply :func:`_old_to_new` conversion,
        and write back (unless *dry_run* is ``True``).

        Returns a list of paths that were (or would be) migrated.
        """
        pattern = os.path.join(self.registry_dir, "**", "*.yaml")
        paths = self.backend.glob(pattern)
        migrated: List[str] = []

        for path in paths:
            data = load_yaml(self.backend, path)

            # Check if migration is needed.
            if "kind" in data:
                # Already has kind — may still benefit from normalisation.
                pass

            new_data = _old_to_new(data.copy())

            # Compare to see if anything changed.
            if new_data == data:
                continue

            migrated.append(path)
            if not dry_run:
                # Write back in-place.
                write_yaml(self.backend, path, yaml_to_config(new_data))
                logger.info("Migrated %s", path)

        return migrated
