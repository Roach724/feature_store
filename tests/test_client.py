import os
import tempfile
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from feature_store.client import FeatureStoreClient
from feature_store.types import EntityKind
from feature_store.schema import (
    FeatureViewConfig, ModelConfig, DatasetConfig,
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
