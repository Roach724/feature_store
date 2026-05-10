# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-05-10

### Added
- Unified YAML format with `kind` field across all entity types
  (`feature_view`, `model`, `label`, `dataset`, `training_set`)
- Integrated registration API: `register()`, `register_feature_view()`,
  `register_model()`, `register_label()`, `register_dataset()`,
  `register_training_set()`
- Pluggable storage backend abstraction (`LocalBackend`, `GCSBackend`,
  `get_backend()`)
- `CheckpointContext` and `checkpoint_context()` context manager for shared
  checkpoint lifecycle across `get_model_features` and `export_training_dataset`
- Auto-register on first write (`auto_register=True` by default on
  `write_entity`, `write_dataset`, `write_training_set`)
- Metadata inference helpers for primary keys, partition columns, and storage
  paths
- `build_dependency_graph()` for recursive BFS dependency DAG traversal
- `migrate_registry()` for batch old-to-new YAML format migration
- `sync_lifecycle()` for extracting and applying GCS bucket lifecycle rules
- `Lookback` dataclass for structured time-shift control in
  `export_training_dataset`
- Structured `export_training_dataset` lookback using `pandas.DateOffset`
- CI/CD pipeline specification with lint, typecheck, test, build, and release
- Build script (`scripts/build.py`) producing wheel and zip artifacts
- 88 unit and integration tests across all modules
- Type-safe config dataclasses with `__post_init__` validation
- Exception hierarchy (`FeatureStoreError`, `EntityNotFoundError`, etc.)
- Python `logging` throughout (replacing all `print()` calls)

### Changed
- **Project structure:** flattened `core/` + `scripts/` → `src/feature_store/`
- **Model features:** URN list `"view@v1:feat"` → structured `schema` with
  `feature_view` / `feature_view_version` per column
- **YAML format:** `primary_keys` and `partition_columns` extracted from inline
  schema to top-level fields with `name` / `type` / `format`
- **Entity identification:** `entity_type: str` → `EntityKind` enum
- **Model YAML:** now requires `version` field (was missing)
- **Model assembly:** `get_batch_model_features` → `get_model_features` with
  internally managed checkpoint lifecycle
- **Registration:** standalone script functions → integrated `client.register()`
- `FeatureStoreClient.__init__`: `feature_store: str` → `registry_dir: str`

### Removed
- `core/` directory (old `client.py`, `yaml_builder.py`, `tools/`)
- `scripts/` directory (old `sync_lifecycle.py`)
- `sync_project_resources.sh` deployment script
- `entity` field in YAML (ambiguous domain entity concept)
- `print()`-based logging throughout
