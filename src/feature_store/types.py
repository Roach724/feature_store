"""Enums and constants for the feature store client."""

import enum


class EntityKind(enum.Enum):
    FEATURE_VIEW = "feature_view"
    MODEL = "model"
    LABEL = "label"
    DATASET = "dataset"
    TRAINING_SET = "training_set"


class StorageFormat(enum.Enum):
    PARQUET = "parquet"


class UpdateFrequency(enum.Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
