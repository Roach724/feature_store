import os
import yaml

from feature_store.registry import yaml_to_config
from feature_store.schema import FeatureViewConfig, ModelConfig, LabelConfig

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
OLD = os.path.join(FIXTURES_DIR, "old_format")
NEW = os.path.join(FIXTURES_DIR, "new_format")


def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class TestOldFormatFixtures:
    def test_feature_view_has_no_kind(self):
        data = _load(os.path.join(OLD, "feature_view.yaml"))
        assert "kind" not in data
        assert data["name"] == "test_features"
        assert data["entity"] == "customers"

    def test_feature_view_has_schema_with_is_primary_key(self):
        data = _load(os.path.join(OLD, "feature_view.yaml"))
        schema = data["schema"]
        pk_cols = [c for c in schema if c.get("is_primary_key")]
        assert len(pk_cols) == 1
        assert pk_cols[0]["name"] == "user_id"

    def test_model_has_features_list(self):
        data = _load(os.path.join(OLD, "model_feature.yaml"))
        assert "features" in data
        assert isinstance(data["features"], list)
        assert "version" not in data

    def test_model_features_are_urns(self):
        data = _load(os.path.join(OLD, "model_feature.yaml"))
        for urn in data["features"]:
            assert ":" in urn
            assert "@" in urn

    def test_label_has_primary_keys_in_schema(self):
        data = _load(os.path.join(OLD, "label.yaml"))
        pk_cols = [c for c in data["schema"] if c.get("is_primary_key")]
        assert len(pk_cols) == 2

    def test_label_has_is_label_column(self):
        data = _load(os.path.join(OLD, "label.yaml"))
        label_cols = [c for c in data["schema"] if c.get("is_label")]
        assert len(label_cols) == 1
        assert label_cols[0]["name"] == "acq_pcd"


class TestNewFormatFixtures:
    def test_feature_view_has_kind(self):
        data = _load(os.path.join(NEW, "feature_view.yaml"))
        assert data["kind"] == "feature_view"
        assert "primary_keys" in data
        assert len(data["primary_keys"]) == 1
        assert data["primary_keys"][0]["name"] == "user_id"

    def test_feature_view_schema_no_primary_keys(self):
        data = _load(os.path.join(NEW, "feature_view.yaml"))
        for col in data["schema"]:
            assert "is_primary_key" not in col

    def test_model_has_schema_not_features(self):
        data = _load(os.path.join(NEW, "model_feature.yaml"))
        assert data["kind"] == "model"
        assert "schema" in data
        assert "features" not in data

    def test_model_schema_has_feature_view_refs(self):
        data = _load(os.path.join(NEW, "model_feature.yaml"))
        assert data["schema"][0]["feature_view"] == "test_features"
        assert data["schema"][0]["feature_view_version"] == "v1"

    def test_label_has_primary_keys_top_level(self):
        data = _load(os.path.join(NEW, "label.yaml"))
        assert data["kind"] == "label"
        assert len(data["primary_keys"]) == 2

    def test_label_schema_has_is_label(self):
        data = _load(os.path.join(NEW, "label.yaml"))
        label_cols = [c for c in data["schema"] if c.get("is_label")]
        assert len(label_cols) == 1


class TestFormatMigrationMapping:
    """Verify the logical mapping from old to new format."""

    def test_old_pk_becomes_new_top_level(self):
        old = _load(os.path.join(OLD, "feature_view.yaml"))
        new = _load(os.path.join(NEW, "feature_view.yaml"))
        old_pks = {c["name"] for c in old["schema"] if c.get("is_primary_key")}
        new_pks = {c["name"] for c in new["primary_keys"]}
        assert old_pks == new_pks

    def test_partition_column_extracted(self):
        old = _load(os.path.join(OLD, "feature_view.yaml"))
        new = _load(os.path.join(NEW, "feature_view.yaml"))
        assert old["storage"]["partition_column"] == new["partition_columns"][0]["name"]

    def test_schema_no_longer_contains_keys_or_partitions(self):
        new = _load(os.path.join(NEW, "feature_view.yaml"))
        pk_names = {c["name"] for c in new["primary_keys"]}
        part_names = {c["name"] for c in new["partition_columns"]}
        schema_names = {c["name"] for c in new["schema"]}
        assert not (pk_names & schema_names)
        assert not (part_names & schema_names)


class TestMigrationRoundtrip:
    """Test that new format fixtures can be loaded by the registry."""

    def test_new_feature_view_loads(self):
        data = _load(os.path.join(NEW, "feature_view.yaml"))
        cfg = yaml_to_config(data)
        assert isinstance(cfg, FeatureViewConfig)
        assert cfg.name == "test_features"

    def test_new_model_loads(self):
        data = _load(os.path.join(NEW, "model_feature.yaml"))
        cfg = yaml_to_config(data)
        assert isinstance(cfg, ModelConfig)
        assert cfg.schema[0].feature_view == "test_features"

    def test_new_label_loads(self):
        data = _load(os.path.join(NEW, "label.yaml"))
        cfg = yaml_to_config(data)
        assert isinstance(cfg, LabelConfig)
        label_cols = [c for c in cfg.schema if c.is_label]
        assert len(label_cols) == 1
        assert label_cols[0].name == "acq_pcd"
