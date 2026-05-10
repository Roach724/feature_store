import os
import sys
import tempfile
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from feature_store.client import CheckpointContext, FeatureStoreClient
from feature_store.types import EntityKind
from feature_store.schema import (
    FeatureViewConfig, ModelConfig, DatasetConfig, LabelConfig,
    KeySpec, ColumnSpec, StorageSpec,
)


@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("feature_store_test") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
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
            name="user_features", version="v1",
            primary_keys=[KeySpec(name="user_id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="/tmp/features/user/v1"),
            schema=[ColumnSpec(name="age", type="integer"), ColumnSpec(name="score", type="double")],
        )
        path = client.register(config)
        assert path.endswith(".yaml")
        assert os.path.exists(path)

    def test_register_feature_view_from_df(self, client, spark_session):
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 25, 0.8)], ["user_id", "dt", "age", "score"]
        )
        path = client.register(
            source=df, kind=EntityKind.FEATURE_VIEW,
            name="df_features", version="v1",
            primary_keys=["user_id"], partition_columns=["dt"],
            storage_base_path="/tmp/features/df/v1",
        )
        assert os.path.exists(path)

    def test_register_model_from_config(self, client):
        config = ModelConfig(
            name="my_model", version="v1",
            primary_keys=[KeySpec(name="user_id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="age", type="integer", feature_view="user_features", feature_view_version="v1"),
            ],
        )
        path = client.register(config)
        assert os.path.exists(path)


class TestWriteAndRead:
    def test_write_and_read_feature_view(self, client, spark_session, tmp_dir):
        base_path = os.path.join(tmp_dir, "data", "test_fv", "v1")
        config = FeatureViewConfig(
            name="test_fv", version="v1",
            primary_keys=[KeySpec(name="id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=base_path),
            schema=[ColumnSpec(name="val", type="double")],
        )
        client.register(config)
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5), (2, "2024-01-01", 0.8)],
            ["id", "dt", "val"]
        )
        client.write_entity(df, EntityKind.FEATURE_VIEW, "test_fv", "v1")
        result = client.get_entity(
            EntityKind.FEATURE_VIEW, "test_fv", "v1",
            columns=["val"], start_date="2024-01-01",
        )
        assert result.count() == 2

    def test_write_entity_unregistered_fails(self, client, spark_session):
        df = spark_session.createDataFrame([(1,)], ["x"])
        with pytest.raises(Exception):
            client.write_entity(df, EntityKind.FEATURE_VIEW, "ghost", "v1")


class TestModelFeatures:
    def test_get_model_features(self, client, spark_session, tmp_dir):
        # Setup: register 2 feature views and write data
        fv1_path = os.path.join(tmp_dir, "data", "fv_a", "v1")
        fv2_path = os.path.join(tmp_dir, "data", "fv_b", "v1")

        fv1 = FeatureViewConfig(
            name="fv_a", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=fv1_path),
            schema=[ColumnSpec(name="feat1", type="double")],
        )
        fv2 = FeatureViewConfig(
            name="fv_b", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=fv2_path),
            schema=[ColumnSpec(name="feat2", type="integer")],
        )
        client.register(fv1)
        client.register(fv2)
        client.write_entity(
            spark_session.createDataFrame([(1, "2024-01-01", 0.5)], ["user_id", "dt", "feat1"]),
            EntityKind.FEATURE_VIEW, "fv_a", "v1"
        )
        client.write_entity(
            spark_session.createDataFrame([(1, "2024-01-01", 10)], ["user_id", "dt", "feat2"]),
            EntityKind.FEATURE_VIEW, "fv_b", "v1"
        )

        model = ModelConfig(
            name="test_model", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            schema=[
                ColumnSpec(name="feat1", type="double", feature_view="fv_a", feature_view_version="v1"),
                ColumnSpec(name="feat2", type="integer", feature_view="fv_b", feature_view_version="v1"),
            ],
        )
        client.register(model)

        query_df = spark_session.createDataFrame([(1, "2024-01-01")], ["user_id", "dt"])
        result = client.get_model_features("test_model", "v1", query_df, start_date="2024-01-01")
        assert result.count() == 1
        cols = set(result.columns)
        assert "feat1" in cols
        assert "feat2" in cols


class TestDatasetOperations:
    def test_write_and_get_dataset(self, client, spark_session, tmp_dir):
        base_path = os.path.join(tmp_dir, "data", "ds", "my_ds", "v1")
        config = DatasetConfig(
            name="my_ds", version="v1",
            primary_keys=[KeySpec(name="id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=base_path),
        )
        client.register(config)
        df = spark_session.createDataFrame([(1, "2024-01-01", 0.7)], ["id", "dt", "target"])
        client.write_entity(df, EntityKind.DATASET, "my_ds", "v1")
        result = client.get_entity(EntityKind.DATASET, "my_ds", "v1", start_date="2024-01-01")
        assert result.count() == 1


class TestManagement:
    def test_list_entities(self, client):
        config = FeatureViewConfig(
            name="list_test", version="v1",
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
            name="info_test", version="v2",
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


class TestAutoRegister:
    def test_write_entity_auto_registers_by_default(self, client, spark_session, tmp_path):
        """write_entity should auto-register when entity doesn't exist."""
        base_path = os.path.join(tmp_path, "data", "auto_fv", "v1")
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5)], ["user_id", "dt", "score"]
        )
        client.write_entity(
            df, EntityKind.FEATURE_VIEW, "auto_fv", "v1",
            storage_base_path=base_path,
        )
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
        config = FeatureViewConfig(
            name="existing", version="v1",
            primary_keys=[KeySpec(name="user_id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path=base_path),
            schema=[ColumnSpec(name="score", type="double")],
        )
        client.register(config)
        df = spark_session.createDataFrame(
            [(1, "2024-01-01", 0.5)], ["user_id", "dt", "score"]
        )
        client.write_entity(df, EntityKind.FEATURE_VIEW, "existing", "v1")
        info = client.get_entity_info(EntityKind.FEATURE_VIEW, "existing", "v1")
        assert info.owner == ""  # unchanged from registered config


@pytest.mark.skipif(sys.platform == "win32", reason="setCheckpointDir not supported on Windows")
class TestCheckpointContextManager:
    def test_context_creates_and_cleans(self, spark_session, tmp_path):
        ckpt_base = os.path.join(tmp_path, "ckpt_base")
        os.makedirs(ckpt_base)
        registry_dir = os.path.join(tmp_path, "registry")
        os.makedirs(registry_dir)
        client = FeatureStoreClient(spark=spark_session, registry_dir=registry_dir)

        observed_path = None
        with client.checkpoint_context(ckpt_base) as ctx:
            observed_path = ctx.path
            # context exists during the with block

        # After context exit, checkpoint dir should be cleaned
        assert not os.path.exists(observed_path)

    def test_context_isolated_per_call(self, spark_session, tmp_path):
        ckpt_base = os.path.join(tmp_path, "ckpt_base")
        os.makedirs(ckpt_base)
        registry_dir = os.path.join(tmp_path, "registry")
        os.makedirs(registry_dir)
        client = FeatureStoreClient(spark=spark_session, registry_dir=registry_dir)

        paths = []
        with client.checkpoint_context(ckpt_base) as ctx1:
            paths.append(ctx1.path)
        with client.checkpoint_context(ckpt_base) as ctx2:
            paths.append(ctx2.path)

        assert paths[0] != paths[1]


@pytest.mark.skipif(sys.platform == "win32", reason="setCheckpointDir not supported on Windows")
class TestModelFeaturesWithCheckpointCtx:
    def test_get_model_features_with_checkpoint_ctx(self, client, spark_session, tmp_path):
        from feature_store.schema import FeatureViewConfig, ModelConfig, KeySpec, ColumnSpec, StorageSpec
        from feature_store.types import EntityKind
        from feature_store.storage import LocalBackend

        fv1_path = os.path.join(tmp_path, "data", "fv_ckpt_a", "v1")
        fv2_path = os.path.join(tmp_path, "data", "fv_ckpt_b", "v1")

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
            assert result.count() == 1

        # After context exits, checkpoint should be cleaned
        assert backend.exists(ctx_path) is False


@pytest.mark.skipif(sys.platform == "win32", reason="setCheckpointDir not supported on Windows")
class TestExportWithCheckpointCtx:
    def test_export_training_dataset_with_checkpoint_ctx(self, client, spark_session, tmp_path):
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

        # Setup model
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
