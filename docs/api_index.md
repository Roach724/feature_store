# API Index

Catalog of all public APIs in the Feature Store, organized by module.

## Modules

| Module | Source | Detail Doc | Description |
|--------|--------|------------|-------------|
| `types` | `src/feature_store/types.py` | [api_types.md](api_types.md) | Enums for entity kind, storage format, update frequency |
| `schema` | `src/feature_store/schema.py` | [api_schema.md](api_schema.md) | Typed dataclasses for entity configuration |
| `storage` | `src/feature_store/storage.py` | [api_storage.md](api_storage.md) | Pluggable storage backends (local, GCS) |
| `registry` | `src/feature_store/registry.py` | [api_registry.md](api_registry.md) | YAML serialization, schema validation, lifecycle |
| `client` | `src/feature_store/client.py` | [api_client.md](api_client.md) | FeatureStoreClient — primary user-facing API |

## API Reference by Module

### types

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `EntityKind` | Enum | [api_types.md](api_types.md) | Entity type: feature_view, model, label, dataset, training_set |
| `StorageFormat` | Enum | [api_types.md](api_types.md) | Storage format: parquet |
| `UpdateFrequency` | Enum | [api_types.md](api_types.md) | Update cadence: daily, weekly, monthly |

### schema

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `ColumnSpec` | Dataclass | [api_schema.md](api_schema.md) | Schema column descriptor with optional feature view lineage |
| `KeySpec` | Dataclass | [api_schema.md](api_schema.md) | Primary key or partition column descriptor |
| `Dependency` | Dataclass | [api_schema.md](api_schema.md) | Upstream entity dependency reference |
| `PipelineSpec` | Dataclass | [api_schema.md](api_schema.md) | Pipeline execution and alerting parameters |
| `RetentionSpec` | Dataclass | [api_schema.md](api_schema.md) | Data retention TTL and cold tier settings |
| `StorageSpec` | Dataclass | [api_schema.md](api_schema.md) | Physical storage location and format |
| `Lookback` | Dataclass | [api_schema.md](api_schema.md) | Time window for feature-label point-in-time joins |
| `FeatureViewConfig` | Dataclass | [api_schema.md](api_schema.md) | Feature view entity configuration |
| `ModelConfig` | Dataclass | [api_schema.md](api_schema.md) | Model entity configuration with feature view references |
| `LabelConfig` | Dataclass | [api_schema.md](api_schema.md) | Label entity configuration |
| `DatasetConfig` | Dataclass | [api_schema.md](api_schema.md) | Dataset entity configuration |
| `TrainingSetConfig` | Dataclass | [api_schema.md](api_schema.md) | Alias for DatasetConfig |

### storage

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `StorageBackend` | ABC | [api_storage.md](api_storage.md) | Abstract storage interface |
| `LocalBackend` | Class | [api_storage.md](api_storage.md) | Local filesystem storage via pyarrow/pandas |
| `GCSBackend` | Class | [api_storage.md](api_storage.md) | Google Cloud Storage via Spark native + fsspec |
| `get_backend` | Function | [api_storage.md](api_storage.md) | Factory: returns appropriate backend for a path |

### registry

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `config_to_yaml` | Function | [api_registry.md](api_registry.md) | Serialize a config dataclass to YAML string |
| `yaml_to_config` | Function | [api_registry.md](api_registry.md) | Deserialize YAML dict to typed config object |
| `load_yaml` | Function | [api_registry.md](api_registry.md) | Load and parse a YAML file from a backend |
| `write_yaml` | Function | [api_registry.md](api_registry.md) | Serialize and write config as YAML via a backend |
| `validate_dataframe` | Function | [api_registry.md](api_registry.md) | Validate DataFrame columns against a config |
| `build_config_from_df` | Function | [api_registry.md](api_registry.md) | Infer a typed config from a Spark DataFrame schema |
| `build_model_config` | Function | [api_registry.md](api_registry.md) | Build a ModelConfig from feature references |
| `list_entities` | Function | [api_registry.md](api_registry.md) | Scan registry directory for entity YAML files |
| `sync_lifecycle` | Function | [api_registry.md](api_registry.md) | Apply GCS lifecycle rules from retention configs |

### client

| API | Type | Detail Doc | Description |
|-----|------|------------|-------------|
| `FeatureStoreClient` | Class | [api_client.md](api_client.md) | Main entry point for all feature store operations |
| `CheckpointContext` | Class | [api_client.md](api_client.md) | Holds checkpoint directory path for `checkpoint_context` |
| `FeatureStoreClient.register` | Method | [api_client.md](api_client.md) | Register an entity from config or DataFrame |
| `FeatureStoreClient.register_feature_view` | Method | [api_client.md](api_client.md) | Convenience: register a feature view |
| `FeatureStoreClient.register_model` | Method | [api_client.md](api_client.md) | Convenience: register a model |
| `FeatureStoreClient.register_label` | Method | [api_client.md](api_client.md) | Convenience: register a label |
| `FeatureStoreClient.register_dataset` | Method | [api_client.md](api_client.md) | Convenience: register a dataset |
| `FeatureStoreClient.register_training_set` | Method | [api_client.md](api_client.md) | Convenience: register a training set |
| `FeatureStoreClient.write_entity` | Method | [api_client.md](api_client.md) | Validate and persist a DataFrame |
| `FeatureStoreClient.get_entity` | Method | [api_client.md](api_client.md) | Read entity data with filters and type coercion |
| `FeatureStoreClient.get_model_features` | Method | [api_client.md](api_client.md) | Assemble feature DataFrame from model config |
| `FeatureStoreClient.export_training_dataset` | Method | [api_client.md](api_client.md) | Point-in-time join of features and labels |
| `FeatureStoreClient.write_dataset` | Method | [api_client.md](api_client.md) | Write a dataset entity |
| `FeatureStoreClient.get_dataset` | Method | [api_client.md](api_client.md) | Read a dataset entity |
| `FeatureStoreClient.write_training_set` | Method | [api_client.md](api_client.md) | Write a train/val/test split |
| `FeatureStoreClient.get_training_set` | Method | [api_client.md](api_client.md) | Read a train/val/test split |
| `FeatureStoreClient.checkpoint_context` | Method | [api_client.md](api_client.md) | Context manager for checkpoint directory |
| `FeatureStoreClient.list_entities` | Method | [api_client.md](api_client.md) | List registered entities |
| `FeatureStoreClient.get_entity_info` | Method | [api_client.md](api_client.md) | Return typed config for a registered entity |
| `FeatureStoreClient.sync_lifecycle` | Method | [api_client.md](api_client.md) | Apply GCS lifecycle rules |
| `FeatureStoreClient.build_dependency_graph` | Method | [api_client.md](api_client.md) | BFS traversal of entity dependencies |
| `FeatureStoreClient.migrate_registry` | Method | [api_client.md](api_client.md) | Convert legacy YAML to current schema |

## Exception Hierarchy

| Exception | Module | Parent | Description |
|-----------|--------|--------|-------------|
| `FeatureStoreError` | registry | `Exception` | Base exception for all feature store errors |
| `EntityNotFoundError` | registry | `FeatureStoreError` | Requested entity not in registry |
| `ColumnNotFoundError` | registry | `FeatureStoreError` | Requested column not found |
| `SchemaValidationError` | registry | `FeatureStoreError` | Schema validation failure |
| `MissingColumnsError` | registry | `SchemaValidationError` | DataFrame missing required columns |
| `ExtraColumnsError` | registry | `SchemaValidationError` | DataFrame has undeclared columns |
| `RegistryFormatError` | registry | `FeatureStoreError` | Malformed registry content |
| `StorageError` | registry | `FeatureStoreError` | Storage-level operation failure |
