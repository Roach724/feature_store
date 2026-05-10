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
