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
            name="click_rate", type="double",
            feature_view="user_features", feature_view_version="v1",
        )
        assert col.feature_view == "user_features"
        assert col.feature_view_version == "v1"


class TestKeySpec:
    def test_basic_key(self):
        key = KeySpec(name="user_id", type="string")
        assert key.name == "user_id"
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
            name="my_features", version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://bucket/fv/v1"),
        )
        assert cfg.name == "my_features"
        assert cfg.kind == "feature_view"
        assert cfg.owner == ""
        assert len(cfg.primary_keys) == 1
        assert cfg.storage.base_path == "gs://bucket/fv/v1"

    def test_name_validation(self):
        with pytest.raises(ValueError, match="name must match"):
            FeatureViewConfig(
                name="InvalidName", version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
            )

    def test_primary_keys_required(self):
        with pytest.raises(ValueError, match="primary_keys must not be empty"):
            FeatureViewConfig(
                name="my_features", version="v1",
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
            )

    def test_partition_columns_required(self):
        with pytest.raises(ValueError, match="partition_columns must not be empty"):
            FeatureViewConfig(
                name="my_features", version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
            )

    def test_column_name_overlap_detected(self):
        with pytest.raises(ValueError, match="must not overlap"):
            FeatureViewConfig(
                name="my_features", version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                storage=StorageSpec(base_path="gs://b/v1"),
                schema=[ColumnSpec(name="id", type="string")],
            )

    def test_with_schema_and_dependency(self):
        cfg = FeatureViewConfig(
            name="my_features", version="v2",
            owner="alice@corp.com", description="User features",
            primary_keys=[KeySpec(name="user_id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string", format="yyyy-MM-dd")],
            storage=StorageSpec(base_path="gs://b/feat/v2"),
            schema=[ColumnSpec(name="age", type="integer"), ColumnSpec(name="score", type="double")],
            dependency=[Dependency(name="upstream_fv", version="v1")],
        )
        assert len(cfg.schema) == 2
        assert len(cfg.dependency) == 1
        assert cfg.dependency[0].name == "upstream_fv"


class TestModelConfig:
    def test_minimal_model_config(self):
        cfg = ModelConfig(
            name="my_model", version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
        )
        assert cfg.kind == "model"

    def test_model_schema_cols_require_feature_view(self):
        with pytest.raises(ValueError, match="must have feature_view"):
            ModelConfig(
                name="my_model", version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                schema=[ColumnSpec(name="age", type="integer")],
            )

    def test_model_schema_cols_require_feature_view_version(self):
        with pytest.raises(ValueError, match="must have feature_view_version"):
            ModelConfig(
                name="my_model", version="v1",
                primary_keys=[KeySpec(name="id", type="string")],
                partition_columns=[KeySpec(name="dt", type="string")],
                schema=[ColumnSpec(name="age", type="integer", feature_view="fv")],
            )

    def test_valid_model_with_feature_refs(self):
        cfg = ModelConfig(
            name="my_model", version="v1",
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
            name="my_label", version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/label/v1"),
        )
        assert cfg.kind == "label"


class TestDatasetConfig:
    def test_minimal_dataset_config(self):
        cfg = DatasetConfig(
            name="my_dataset", version="v1",
            primary_keys=[KeySpec(name="id", type="string")],
            partition_columns=[KeySpec(name="dt", type="string")],
            storage=StorageSpec(base_path="gs://b/ds/v1"),
        )
        assert cfg.kind == "dataset"
