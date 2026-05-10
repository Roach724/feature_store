# Checkpoint Context & Auto-register — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add checkpoint context manager to solve premature cleanup in model assembly + export pipeline, and auto-register-on-write to eliminate the separate registration step.

**Architecture:** A `CheckpointContext` wrapper class owned by a `checkpoint_context()` context manager on the client. `get_model_features` and `export_training_dataset` accept an optional `checkpoint_ctx`. `write_entity` gains `auto_register=True` with inference fallbacks for metadata.

**Tech Stack:** Python 3.10+, PySpark, existing project at `src/feature_store/`

---

## File Structure

```
Modified:
  src/feature_store/client.py    # CheckpointContext, checkpoint_context(), updated methods
  tests/test_client.py           # New test classes

No new files created.
```

---

### Task 1: CheckpointContext + checkpoint_context() + updated get_model_features

**Files:**
- Modify: `src/feature_store/client.py` — add class, context manager, update get_model_features
- Modify: `tests/test_client.py` — add checkpoint-related tests

- [ ] **Step 1: Write failing tests**

Add to `D:/feature_store/tests/test_client.py`:

```python
import os
from feature_store.client import CheckpointContext


class TestCheckpointContext:
    def test_cleanup(self, tmp_path):
        from feature_store.storage import LocalBackend
        backend = LocalBackend()
        ckpt_dir = os.path.join(tmp_path, "ckpt")
        os.makedirs(os.path.join(ckpt_dir, "sub"))
        with open(os.path.join(ckpt_dir, "f.txt"), "w") as f:
            f.write("data")

        ctx = CheckpointContext(backend, ckpt_dir)
        assert os.path.exists(ckpt_dir)
        ctx.cleanup()
        assert not os.path.exists(ckpt_dir)

    def test_cleanup_idempotent(self, tmp_path):
        from feature_store.storage import LocalBackend
        backend = LocalBackend()
        ctx = CheckpointContext(backend, os.path.join(tmp_path, "nonexistent"))
        ctx.cleanup()  # should not raise
        ctx.cleanup()  # should not raise


class TestCheckpointContextManager:
    def test_context_creates_and_cleans(self, spark_session, tmp_path):
        """Verify checkpoint_context yields a path that exists during the
        block and is cleaned after exit."""
        ckpt_base = os.path.join(tmp_path, "ckpt_base")
        os.makedirs(ckpt_base)

        # Need a minimal client for the context manager
        registry_dir = os.path.join(tmp_path, "registry")
        os.makedirs(registry_dir)
        from feature_store.client import FeatureStoreClient
        client = FeatureStoreClient(spark=spark_session, registry_dir=registry_dir)

        observed_path = None
        with client.checkpoint_context(ckpt_base) as ctx:
            observed_path = ctx.path
            assert os.path.exists(observed_path) is False  # dir created but empty
            # Spark setCheckpointDir was called — verify by checking Spark conf
            # (SparkContext.setCheckpointDir doesn't expose a getter, skip)

        # After context exit, checkpoint dir should be cleaned
        assert not os.path.exists(observed_path)

    def test_context_isolated_per_call(self, spark_session, tmp_path):
        """Each call to checkpoint_context should create a unique directory."""
        ckpt_base = os.path.join(tmp_path, "ckpt_base")
        os.makedirs(ckpt_base)
        registry_dir = os.path.join(tmp_path, "registry")
        os.makedirs(registry_dir)
        from feature_store.client import FeatureStoreClient
        client = FeatureStoreClient(spark=spark_session, registry_dir=registry_dir)

        paths = []
        with client.checkpoint_context(ckpt_base) as ctx1:
            paths.append(ctx1.path)
        with client.checkpoint_context(ckpt_base) as ctx2:
            paths.append(ctx2.path)

        assert paths[0] != paths[1]


class TestModelFeaturesWithCheckpointCtx:
    def test_get_model_features_with_checkpoint_ctx(self, client, spark_session, tmp_path):
        """get_model_features should accept a CheckpointContext and not
        clean it up (the context manager owns cleanup)."""
        # Setup: register and write 2 feature views + 1 model
        fv1_path = os.path.join(tmp_path, "data", "fv_ckpt_a", "v1")
        fv2_path = os.path.join(tmp_path, "data", "fv_ckpt_b", "v1")
        from feature_store.schema import FeatureViewConfig, ModelConfig, KeySpec, ColumnSpec, StorageSpec
        from feature_store.types import EntityKind
        from feature_store.client import CheckpointContext
        from feature_store.storage import LocalBackend

        fv1 = FeatureViewConfig(
            name="fv_ckpt_a", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=fv1_path),
            schema=[ColumnSpec(name="feat_a", type="double")],
        )
        fv2 = FeatureViewConfig(
            name="fv_ckpt_b", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=fv2_path),
            schema=[ColumnSpec(name="feat_b", type="integer")],
        )
        client.register(fv1)
        client.register(fv2)
        client.write_entity(
            spark_session.createDataFrame([(1, "2024-01-01", 0.5)], ["user_id", "dt", "feat_a"]),
            EntityKind.FEATURE_VIEW, "fv_ckpt_a", "v1",
        )
        client.write_entity(
            spark_session.createDataFrame([(1, "2024-01-01", 10)], ["user_id", "dt", "feat_b"]),
            EntityKind.FEATURE_VIEW, "fv_ckpt_b", "v1",
        )
        model = ModelConfig(
            name="model_ckpt", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="feat_a", type="double", feature_view="fv_ckpt_a", feature_view_version="v1"),
                ColumnSpec(name="feat_b", type="integer", feature_view="fv_ckpt_b", feature_view_version="v1"),
            ],
        )
        client.register(model)

        query_df = spark_session.createDataFrame([(1, "2024-01-01")], ["user_id", "dt"])

        ckpt_base = os.path.join(tmp_path, "ckpt_base")
        os.makedirs(ckpt_base)

        backend = LocalBackend()
        ctx_path = None
        with client.checkpoint_context(ckpt_base) as ctx:
            ctx_path = ctx.path
            result = client.get_model_features(
                "model_ckpt", "v1", query_df, start_date="2024-01-01",
                checkpoint_ctx=ctx, checkpoint_interval=1,
            )
            # At this point, the context is still open — checkpoint should exist
            assert result.count() == 1

        # After context exits, checkpoint should be cleaned
        assert not os.path.exists(ctx_path)
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py -v -k "TestCheckpointContext or TestCheckpointContextManager or TestModelFeaturesWithCheckpointCtx"
```
Expected: FAIL — `ImportError: cannot import name 'CheckpointContext'`

- [ ] **Step 3: Implement CheckpointContext class and checkpoint_context() method**

In `D:/feature_store/src/feature_store/client.py`:

Add the import at top (near existing imports):
```python
from contextlib import contextmanager
```

Add the `CheckpointContext` class before `FeatureStoreClient`:
```python
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
```

Add `checkpoint_context` method to `FeatureStoreClient` (after `__init__`):
```python
    @contextmanager
    def checkpoint_context(self, base_dir: str):
        """Context manager that creates a unique checkpoint directory, sets it
        on the SparkContext, and guarantees cleanup on exit.

        Usage::

            with client.checkpoint_context("/tmp/checkpoints") as ctx:
                dataset = client.export_training_dataset(
                    ..., checkpoint_ctx=ctx, dry_run=False, output_path="..."
                )
            # checkpoint cleaned here
        """
        path = f"{base_dir}/{uuid.uuid4().hex[:8]}"
        self.spark.sparkContext.setCheckpointDir(path)
        ctx = CheckpointContext(self.backend, path)
        logger.info("Checkpoint context created: %s", path)
        try:
            yield ctx
        finally:
            ctx.cleanup()
            logger.debug("Checkpoint context cleaned: %s", path)
```

- [ ] **Step 4: Update get_model_features signature and logic**

Replace the `get_model_features` method signature:
```python
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
```

Replace the checkpoint setup/cleanup block (the part that sets `checkpoint_path` and the `try/finally`) with:

```python
        total_joins = len(view_groups)

        # Determine checkpoint strategy
        checkpoint_path = None
        owns_checkpoint = False

        if checkpoint_ctx is not None:
            # Context manager owns lifecycle — just use its path
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
```

And replace the `finally` block:
```python
        try:
            result_df = query_df
            join_count = 0
            # ... (join loop unchanged) ...
            return result_df
        finally:
            if owns_checkpoint and checkpoint_path:
                self.backend.rm(checkpoint_path, recursive=True)
                logger.debug("Checkpoint cleaned: %s", checkpoint_path)
```

- [ ] **Step 5: Run tests**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py -v -k "TestCheckpointContext or TestCheckpointContextManager or TestModelFeaturesWithCheckpointCtx"
```
Expected: All new tests PASS

- [ ] **Step 6: Run all existing tests to verify no regressions**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v --tb=short
```
Expected: All ~80 tests PASS

- [ ] **Step 7: Commit**

```bash
cd D:/feature_store && git add src/feature_store/client.py tests/test_client.py && git commit -m "feat: add CheckpointContext and checkpoint_context() to solve premature checkpoint cleanup"
```

---

### Task 2: Forward checkpoint_ctx in export_training_dataset

**Files:**
- Modify: `src/feature_store/client.py` — export_training_dataset signature + forwarding
- Modify: `tests/test_client.py` — add export+checkpoint integration test

- [ ] **Step 1: Write failing test**

Add to `D:/feature_store/tests/test_client.py`:

```python
class TestExportWithCheckpointCtx:
    def test_export_training_dataset_with_checkpoint_ctx(self, client, spark_session, tmp_path):
        """Full pipeline: register fv + label + model, export with checkpoint_ctx."""
        from feature_store.schema import FeatureViewConfig, ModelConfig, LabelConfig, KeySpec, ColumnSpec, StorageSpec
        from feature_store.types import EntityKind

        # Setup feature view
        fv_path = os.path.join(tmp_path, "data", "fv_exp", "v1")
        fv = FeatureViewConfig(
            name="fv_exp", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=fv_path),
            schema=[ColumnSpec(name="feat1", type="double")],
        )
        client.register(fv)
        client.write_entity(
            spark_session.createDataFrame([(1, "2024-01-01", 0.5)], ["user_id", "dt", "feat1"]),
            EntityKind.FEATURE_VIEW, "fv_exp", "v1",
        )

        # Setup label
        label_path = os.path.join(tmp_path, "data", "lbl_exp", "v1")
        label = LabelConfig(
            name="lbl_exp", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=label_path),
            schema=[ColumnSpec(name="target", type="integer", is_label=True)],
        )
        client.register(label)
        client.write_entity(
            spark_session.createDataFrame([(1, "2024-01-01", 1)], ["user_id", "dt", "target"]),
            EntityKind.LABEL, "lbl_exp", "v1",
        )

        # Setup model (small — 1 feature view, no checkpoint needed, but with ctx it should still work)
        model = ModelConfig(
            name="model_exp", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="feat1", type="double", feature_view="fv_exp", feature_view_version="v1"),
            ],
        )
        client.register(model)

        query_df = spark_session.createDataFrame([(1, "2024-01-01")], ["user_id", "dt"])

        ckpt_base = os.path.join(tmp_path, "ckpt_base")
        os.makedirs(ckpt_base)
        output_path = os.path.join(tmp_path, "output")

        ctx_path = None
        with client.checkpoint_context(ckpt_base) as ctx:
            ctx_path = ctx.path
            dataset = client.export_training_dataset(
                query_df=query_df,
                model_name="model_exp", model_version="v1",
                label_name="lbl_exp", label_version="v1",
                feature_start_date="2024-01-01",
                checkpoint_ctx=ctx,
                dry_run=False,
                output_path=output_path,
            )
            assert dataset.count() == 1

        # Checkpoint cleaned after context exit
        assert not os.path.exists(ctx_path)
        # Output written
        assert os.path.exists(output_path)
```

- [ ] **Step 2: Run test to verify failure**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py::TestExportWithCheckpointCtx -v
```
Expected: FAIL — `export_training_dataset() got an unexpected keyword argument 'checkpoint_ctx'`

- [ ] **Step 3: Update export_training_dataset**

Change signature to add `checkpoint_ctx=None`:

```python
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
```

In the call to `self.get_model_features(...)` inside this method, add `checkpoint_ctx=checkpoint_ctx`:
```python
        feature_df = self.get_model_features(
            model_name, model_version, query_df,
            start_date=feature_start_date, end_date=feature_end_date,
            checkpoint_ctx=checkpoint_ctx,
        )
```

- [ ] **Step 4: Run test**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py::TestExportWithCheckpointCtx -v
```
Expected: PASS

- [ ] **Step 5: Run all tests**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v --tb=short
```

- [ ] **Step 6: Commit**

```bash
cd D:/feature_store && git add src/feature_store/client.py tests/test_client.py && git commit -m "feat: forward checkpoint_ctx in export_training_dataset for end-to-end checkpoint lifecycle"
```

---

### Task 3: Auto-register in write_entity

**Files:**
- Modify: `src/feature_store/client.py` — write_entity signature, inference helpers, auto-register logic
- Modify: `tests/test_client.py` — add auto-register tests

- [ ] **Step 1: Write failing tests**

Add to `D:/feature_store/tests/test_client.py`:

```python
class TestAutoRegister:
    def test_write_entity_auto_registers_by_default(self, client, spark_session, tmp_path):
        """write_entity should auto-register when entity doesn't exist."""
        base_path = os.path.join(tmp_path, "data", "auto_fv", "v1")
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5)], ["user_id", "dt", "score"]
        )
        # Write without pre-registering — should auto-register
        client.write_entity(
            df, EntityKind.FEATURE_VIEW, "auto_fv", "v1",
            storage_base_path=base_path,
        )
        # Verify the entity is now registered
        info = client.get_entity_info(EntityKind.FEATURE_VIEW, "auto_fv", "v1")
        assert info.name == "auto_fv"
        assert info.kind == "feature_view"

    def test_write_entity_auto_register_disabled(self, client, spark_session, tmp_path):
        """write_entity with auto_register=False should raise EntityNotFoundError."""
        df = spark_session.createDataFrame([(1,)], ["x"])
        with pytest.raises(Exception):
            client.write_entity(
                df, EntityKind.FEATURE_VIEW, "never_registered", "v1",
                auto_register=False,
            )

    def test_write_entity_infers_primary_keys(self, spark_session, tmp_path):
        """Auto-register should infer primary_keys from columns ending in _id."""
        registry_dir = os.path.join(tmp_path, "registry")
        os.makedirs(registry_dir)
        from feature_store.client import FeatureStoreClient
        client = FeatureStoreClient(spark=spark_session, registry_dir=registry_dir)

        base_path = os.path.join(tmp_path, "data", "infer_pk", "v1")
        df = spark_session.createDataFrame(
            [(1, "a", "2024-01-01", 0.5)],
            ["account_id", "name", "dt", "score"]
        )
        client.write_entity(
            df, EntityKind.FEATURE_VIEW, "infer_pk", "v1",
            storage_base_path=base_path,
        )
        info = client.get_entity_info(EntityKind.FEATURE_VIEW, "infer_pk", "v1")
        pk_names = {k.name for k in info.primary_keys}
        assert "account_id" in pk_names

    def test_write_entity_explicit_metadata_overrides_inference(self, spark_session, tmp_path):
        """Explicit primary_keys/partition_columns should override inference."""
        registry_dir = os.path.join(tmp_path, "registry")
        os.makedirs(registry_dir)
        from feature_store.client import FeatureStoreClient
        client = FeatureStoreClient(spark=spark_session, registry_dir=registry_dir)

        base_path = os.path.join(tmp_path, "data", "explicit", "v1")
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5)], ["custom_key", "custom_dt", "score"]
        )
        client.write_entity(
            df, EntityKind.FEATURE_VIEW, "explicit", "v1",
            primary_keys=["custom_key"],
            partition_columns=["custom_dt"],
            storage_base_path=base_path,
        )
        info = client.get_entity_info(EntityKind.FEATURE_VIEW, "explicit", "v1")
        assert info.primary_keys[0].name == "custom_key"
        assert info.partition_columns[0].name == "custom_dt"

    def test_write_dataset_auto_registers(self, client, spark_session, tmp_path):
        """write_dataset should also auto-register."""
        base_path = os.path.join(tmp_path, "data", "auto_ds", "v1")
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.7)], ["user_id", "dt", "target"]
        )
        client.write_dataset(
            df, "auto_ds", "v1",
            storage_base_path=base_path,
        )
        info = client.get_entity_info(EntityKind.DATASET, "auto_ds", "v1")
        assert info.name == "auto_ds"

    def test_write_entity_already_registered_no_duplicate(self, client, spark_session, tmp_path):
        """When entity is already registered, auto_register is skipped."""
        base_path = os.path.join(tmp_path, "data", "existing", "v1")
        # Pre-register
        config = FeatureViewConfig(
            name="existing", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=base_path),
            schema=[ColumnSpec(name="score", type="double")],
        )
        client.register(config)

        # Write with auto_register=True — should use existing config, not override
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5)], ["user_id", "dt", "score"]
        )
        client.write_entity(df, EntityKind.FEATURE_VIEW, "existing", "v1")
        info = client.get_entity_info(EntityKind.FEATURE_VIEW, "existing", "v1")
        assert info.owner == ""  # unchanged from registered config
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py -v -k "TestAutoRegister"
```
Expected: FAIL — `EntityNotFoundError` on first test

- [ ] **Step 3: Add inference helpers to client.py**

Add these module-level functions before `FeatureStoreClient`:

```python
def _infer_primary_keys(df) -> List[str]:
    """Infer primary key columns from DataFrame columns."""
    keys = [c for c in df.columns if c.endswith("_id") or c == "id"]
    return keys if keys else []


def _infer_partition_columns(df) -> List[str]:
    """Infer partition columns from common names."""
    for candidate in ["dt", "partition_date", "feature_month", "label_month"]:
        if candidate in df.columns:
            return [candidate]
    return ["dt"]


def _infer_storage_path(registry_dir: str, kind: EntityKind, name: str, version: str) -> str:
    """Infer a storage base path from the registry directory."""
    base = registry_dir.replace("/registry", "").replace("\\registry", "")
    kind_map = {
        EntityKind.FEATURE_VIEW: "feature_views",
        EntityKind.MODEL: "models",
        EntityKind.LABEL: "labels",
        EntityKind.DATASET: "datasets",
        EntityKind.TRAINING_SET: "training_sets",
    }
    return f"{base}/{kind_map.get(kind, 'entities')}/{name}/{version}"
```

- [ ] **Step 4: Update write_entity signature and logic**

Change signature:
```python
    def write_entity(
        self,
        df,
        kind: EntityKind,
        name: str,
        version: str,
        partition_num: int = 24,
        allow_extra_columns: bool = False,
        auto_register: bool = True,
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        storage_base_path: Optional[str] = None,
        **kwargs,
    ) -> None:
```

Replace the `_load_config` call with try/except logic:
```python
        # Try to load existing config; auto-register if not found.
        try:
            config = self._load_config(kind, name, version)
        except EntityNotFoundError:
            if not auto_register:
                raise
            # Resolve registration metadata
            resolved_pk = primary_keys or _infer_primary_keys(df)
            resolved_pc = partition_columns or _infer_partition_columns(df)
            resolved_storage = storage_base_path or _infer_storage_path(
                self.registry_dir, kind, name, version
            )
            logger.info(
                "Auto-registering %s/%s/%s (primary_keys=%s, partition_columns=%s, storage=%s)",
                kind.value, name, version, resolved_pk, resolved_pc, resolved_storage,
            )
            self.register(
                df,
                kind=kind,
                name=name,
                version=version,
                primary_keys=resolved_pk,
                partition_columns=resolved_pc,
                storage_base_path=resolved_storage,
                **kwargs,
            )
            config = self._load_config(kind, name, version)
```

- [ ] **Step 5: Run auto-register tests**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/test_client.py -v -k "TestAutoRegister"
```
Expected: All PASS

- [ ] **Step 6: Run all tests**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v --tb=short
```

- [ ] **Step 7: Commit**

```bash
cd D:/feature_store && git add src/feature_store/client.py tests/test_client.py && git commit -m "feat: add auto-register on first write with metadata inference"
```

---

### Task 4: Forward auto_register in write_dataset and write_training_set

**Files:**
- Modify: `src/feature_store/client.py` — write_dataset, write_training_set signatures

- [ ] **Step 1: Update write_dataset**

```python
    def write_dataset(
        self,
        dataset,
        name: str,
        version: str,
        mode: str = "overwrite",
        partition_num: int = 200,
        auto_register: bool = True,
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        storage_base_path: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Write a dataset entity."""
        self.write_entity(
            dataset, EntityKind.DATASET, name, version,
            partition_num=partition_num,
            auto_register=auto_register,
            primary_keys=primary_keys,
            partition_columns=partition_columns,
            storage_base_path=storage_base_path,
            **kwargs,
        )
```

- [ ] **Step 2: Update write_training_set**

```python
    def write_training_set(
        self,
        dataset,
        name: str,
        version: str,
        split: str = "train",
        mode: str = "overwrite",
        auto_register: bool = True,
        primary_keys: Optional[List[str]] = None,
        partition_columns: Optional[List[str]] = None,
        storage_base_path: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Write a training / validation / test split."""
        # Auto-register check
        try:
            self._load_config(EntityKind.TRAINING_SET, name, version)
        except EntityNotFoundError:
            if not auto_register:
                raise
            resolved_pk = primary_keys or _infer_primary_keys(dataset)
            resolved_pc = partition_columns or _infer_partition_columns(dataset)
            resolved_storage = storage_base_path or _infer_storage_path(
                self.registry_dir, EntityKind.TRAINING_SET, name, version
            )
            self.register(
                dataset, kind=EntityKind.TRAINING_SET, name=name, version=version,
                primary_keys=resolved_pk, partition_columns=resolved_pc,
                storage_base_path=resolved_storage, **kwargs,
            )

        config = self._load_config(EntityKind.TRAINING_SET, name, version)
        base_path = (config.storage.base_path.replace("\\", "/") + "/" + split)
        partition_cols = [k.name for k in config.partition_columns]
        self.backend.write_parquet(
            dataset,
            base_path,
            partition_cols=partition_cols,
            mode=mode,
        )
```

- [ ] **Step 3: Run all tests**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v --tb=short
```
Expected: All ~86 tests PASS

- [ ] **Step 4: Commit**

```bash
cd D:/feature_store && git add src/feature_store/client.py && git commit -m "feat: forward auto_register params in write_dataset and write_training_set"
```

---

### Task 5: Final Verification

- [ ] **Step 1: Run full test suite**

```bash
cd D:/feature_store && PYTHONPATH=src python -m pytest tests/ -v --tb=short
```

- [ ] **Step 2: Verify git log**

```bash
cd D:/feature_store && git log --oneline -5
```
