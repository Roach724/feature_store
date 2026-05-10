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
        name="test_features", version="v1",
        owner="alice@corp.com", description="Test feature view",
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
        assert len(data["partition_columns"]) == 1
        assert data["partition_columns"][0]["format"] == "yyyy-MM-dd"
        assert "storage" in data
        assert data["storage"]["base_path"] == "gs://bucket/features/test/v1"
        assert len(data["schema"]) == 2

    def test_model_config_to_yaml(self):
        cfg = ModelConfig(
            name="my_model", version="v2",
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
        assert len(data["dependency"]) == 1

    def test_label_config_to_yaml(self):
        cfg = LabelConfig(
            name="my_label", version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/l/v1"),
            schema=[ColumnSpec(name="churn", type="integer", is_label=True)],
        )
        yaml_str = config_to_yaml(cfg)
        data = yaml.safe_load(yaml_str)
        assert data["kind"] == "label"
        assert data["schema"][0]["is_label"] is True

    def test_dataset_config_to_yaml(self):
        cfg = DatasetConfig(
            name="my_dataset", version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/ds/v1"),
            dependency=[Dependency(name="model1", version="v1")],
        )
        yaml_str = config_to_yaml(cfg)
        data = yaml.safe_load(yaml_str)
        assert data["kind"] == "dataset"
        assert "schema" not in data

    def test_feature_view_without_optional_fields(self):
        cfg = FeatureViewConfig(
            name="minimal", version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/min/v1"),
        )
        yaml_str = config_to_yaml(cfg)
        data = yaml.safe_load(yaml_str)
        assert data["kind"] == "feature_view"
        assert "pipeline" in data  # has default
        assert "retention" in data  # has default


class TestYamlToConfig:
    def test_feature_view_from_dict(self):
        data = {
            "kind": "feature_view", "name": "fv", "version": "v1",
            "owner": "a@b.com",
            "primary_keys": [{"name": "id", "type": "string"}],
            "partition_columns": [{"name": "dt", "type": "string"}],
            "storage": {"base_path": "gs://b/fv/v1"},
            "schema": [{"name": "age", "type": "integer"}],
        }
        cfg = yaml_to_config(data)
        assert isinstance(cfg, FeatureViewConfig)
        assert cfg.name == "fv"
        assert len(cfg.schema) == 1

    def test_model_from_dict(self):
        data = {
            "kind": "model", "name": "m", "version": "v1",
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
            "kind": "label", "name": "lbl", "version": "v1",
            "primary_keys": [{"name": "id", "type": "string"}],
            "partition_columns": [{"name": "dt", "type": "string"}],
            "storage": {"base_path": "gs://b/l/v1"},
            "schema": [{"name": "target", "type": "integer", "is_label": True}],
        }
        cfg = yaml_to_config(data)
        assert isinstance(cfg, LabelConfig)

    def test_dataset_from_dict(self):
        data = {
            "kind": "dataset", "name": "ds", "version": "v1",
            "primary_keys": [{"name": "id", "type": "string"}],
            "partition_columns": [{"name": "dt", "type": "string"}],
            "storage": {"base_path": "gs://b/ds/v1"},
        }
        cfg = yaml_to_config(data)
        assert isinstance(cfg, DatasetConfig)

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="Unknown kind"):
            yaml_to_config({"kind": "unknown_type", "name": "x", "version": "v1"})

    def test_missing_kind_raises(self):
        with pytest.raises(ValueError, match="Missing 'kind'"):
            yaml_to_config({"name": "x", "version": "v1"})

    def test_full_yaml_roundtrip(self, feature_view_config):
        yaml_str = config_to_yaml(feature_view_config)
        data = yaml.safe_load(yaml_str)
        cfg2 = yaml_to_config(data)
        assert cfg2.name == feature_view_config.name
        assert cfg2.kind == feature_view_config.kind
        assert len(cfg2.schema) == len(feature_view_config.schema)


class TestValidateDataframe:
    @pytest.fixture
    def spark(self):
        pyspark = pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        yield spark
        spark.stop()

    def test_missing_columns(self, spark):
        df = spark.createDataFrame([(1, "a")], ["id", "name"])
        cfg = FeatureViewConfig(
            name="fv", version="v1",
            primary_keys=[KeySpec(name="id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/fv/v1"),
            schema=[ColumnSpec(name="name", type="string"), ColumnSpec(name="missing", type="integer")],
        )
        with pytest.raises(MissingColumnsError, match="missing"):
            validate_dataframe(df, cfg)

    def test_extra_columns(self, spark):
        df = spark.createDataFrame([(1, "a", "extra")], ["id", "name", "ghost"])
        cfg = FeatureViewConfig(
            name="fv", version="v1",
            primary_keys=[KeySpec(name="id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/fv/v1"),
            schema=[ColumnSpec(name="name", type="string")],
        )
        with pytest.raises(ExtraColumnsError, match="unknown"):
            validate_dataframe(df, cfg)

    def test_allow_extra_columns(self, spark):
        df = spark.createDataFrame([(1, "a", "2024-01-01", "extra")], ["id", "name", "dt", "ghost"])
        cfg = FeatureViewConfig(
            name="fv", version="v1",
            primary_keys=[KeySpec(name="id", type="integer")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/fv/v1"),
            schema=[ColumnSpec(name="name", type="string")],
        )
        validate_dataframe(df, cfg, allow_extra_columns=True)


class TestBuildConfigFromDf:
    @pytest.fixture
    def spark(self):
        pyspark = pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        yield spark
        spark.stop()

    def test_feature_view_from_df(self, spark):
        df = spark.createDataFrame(
            [(1, "aaa", "2024-01-01", 25, 0.8)],
            ["user_id", "name", "dt", "age", "score"]
        )
        cfg = build_config_from_df(
            df=df, kind=EntityKind.FEATURE_VIEW,
            name="my_fv", version="v1",
            primary_keys=["user_id"], partition_columns=["dt"],
            storage_base_path="gs://bucket/fv/v1",
        )
        assert cfg.name == "my_fv"
        assert cfg.kind == "feature_view"
        assert len(cfg.primary_keys) == 1
        assert cfg.primary_keys[0].name == "user_id"
        schema_names = {c.name for c in cfg.schema}
        assert schema_names == {"name", "age", "score"}
