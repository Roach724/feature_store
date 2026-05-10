# Feature Store Client — Checkpoint & Auto-register Design

Date: 2026-05-10
Status: approved

## Overview

Two improvements to the feature store client:

1. **Checkpoint Context Manager** — solve the issue where `get_model_features` eagerly cleans up
   checkpoint files before downstream Spark actions (in `export_training_dataset`) have materialized
2. **Auto-register on First Write** — `write_entity` (and convenience wrappers) can auto-register
   an entity from the DataFrame when it doesn't yet exist in the registry

---

## 1. Checkpoint Context Manager

### Problem

`get_model_features` creates checkpoint directories to break Spark lineage, but deletes them in a
`finally` block before the caller's downstream Spark actions execute. Since Spark is lazy, the
checkpoint files are referenced by the query plan but no longer exist when the action (e.g.
`write.parquet`) triggers execution.

### Design

A `CheckpointContext` class managed via `FeatureStoreClient.checkpoint_context()` context manager.
The context manager owns the checkpoint directory lifecycle. `get_model_features` and
`export_training_dataset` accept an optional `checkpoint_ctx` parameter — when provided, they use
its checkpoint directory without cleaning up.

#### New class

```python
class CheckpointContext:
    def __init__(self, backend, path: str):
        self.backend = backend
        self.path = path

    def cleanup(self):
        if self.path:
            self.backend.rm(self.path, recursive=True)
            self.path = None
```

#### New client method

```python
from contextlib import contextmanager

class FeatureStoreClient:
    @contextmanager
    def checkpoint_context(self, base_dir: str):
        path = f"{base_dir}/{uuid4().hex[:8]}"
        self.spark.sparkContext.setCheckpointDir(path)
        ctx = CheckpointContext(self.backend, path)
        try:
            yield ctx
        finally:
            ctx.cleanup()
```

#### `get_model_features` signature change

```python
def get_model_features(
    self, model_name, model_version, query_df,
    start_date, end_date=None,
    checkpoint_interval=5,
    checkpoint_dir=None,          # legacy: used when no checkpoint_ctx
    checkpoint_ctx=None,          # NEW: CheckpointContext
):
```

**Logic:**

| Scenario | Checkpoint dir source | Cleanup responsibility |
|----------|----------------------|----------------------|
| `checkpoint_ctx` provided | `checkpoint_ctx.path` | Context manager |
| `checkpoint_dir` provided (legacy) | auto-generated UUID subdir under `checkpoint_dir` | `finally` block in method |
| Neither provided, joins > interval | ValueError (as before) | — |

#### `export_training_dataset` signature change

```python
def export_training_dataset(
    self, query_df, model_name, model_version,
    label_name, label_version, feature_start_date,
    feature_end_date=None, lookback=None,
    join_type="left", dry_run=True, output_path=None,
    checkpoint_ctx=None,          # NEW: forwarded to get_model_features
):
```

Simply forwards `checkpoint_ctx` to `get_model_features`.

#### Usage patterns

**Combined (shared checkpoint — recommended for export):**
```python
with client.checkpoint_context("/tmp/ckpt") as ctx:
    dataset = client.export_training_dataset(
        query_df=query, model_name="m", model_version="v1",
        label_name="l", label_version="v1",
        feature_start_date="2024-01-01",
        checkpoint_ctx=ctx,
        dry_run=False, output_path="gs://bucket/output",
    )
# checkpoint cleaned after context exits → all Spark actions complete
```

**Standalone get_model_features (backward compatible):**
```python
features = client.get_model_features("m", "v1", query, "2024-01-01",
                                      checkpoint_dir="/tmp/ckpt")
# checkpoint self-cleaned in finally
```

**Standalone export (backward compatible, small models):**
```python
dataset = client.export_training_dataset(query, "m", "v1", "l", "v1", "2024-01-01")
# model has ≤5 joins, no checkpoint needed
```

#### Backward compatibility

- `checkpoint_dir` parameter preserved and functional
- `checkpoint_ctx=None` (default) falls through to legacy behavior
- No breaking changes to existing callers

---

## 2. Auto-register on First Write

### Problem

`write_entity` calls `_load_config` which raises `EntityNotFoundError` if the entity hasn't been
registered. Users must call `register()` separately before `write_entity()`, adding boilerplate for
the common case where the entity is being created for the first time.

### Design

`write_entity` gains `auto_register: bool = True` (default) and optional registration metadata
params. When auto-register fires, it infers missing metadata from the DataFrame and convention.

#### `write_entity` new signature

```python
def write_entity(
    self, df, kind, name, version,
    partition_num=24,
    allow_extra_columns=False,
    auto_register=True,
    primary_keys=None,
    partition_columns=None,
    storage_base_path=None,
    **kwargs,
):
```

#### Flow

1. Try `_load_config(kind, name, version)`
2. If found → validate + write (current behavior)
3. If `EntityNotFoundError` AND `auto_register=True`:
   a. Resolve metadata (explicit params > inference)
   b. Call `self.register(df, kind, name, version, primary_keys, partition_columns, storage_base_path)`
   c. Proceed with validate + write
4. If `EntityNotFoundError` AND `auto_register=False` → raise (current behavior)

#### Inference helpers

```python
def _infer_primary_keys(df) -> List[str]:
    return [c for c in df.columns if c.endswith("_id") or c == "id"]

def _infer_partition_columns(df) -> List[str]:
    for candidate in ["dt", "partition_date", "feature_month", "label_month"]:
        if candidate in df.columns:
            return [candidate]
    return ["dt"]

def _infer_storage_path(registry_dir, kind, name, version) -> str:
    base = registry_dir.replace("/registry", "")
    kind_map = {
        EntityKind.FEATURE_VIEW: "feature_views",
        EntityKind.MODEL: "models",
        EntityKind.LABEL: "labels",
        EntityKind.DATASET: "datasets",
        EntityKind.TRAINING_SET: "training_sets",
    }
    return f"{base}/{kind_map[kind]}/{name}/{version}"
```

#### Convenience methods

`write_dataset`, `write_training_set` also gain `auto_register=True` and forward the metadata
params to `write_entity`.

#### Usage

**Explicit metadata (precise control):**
```python
client.write_entity(df, EntityKind.FEATURE_VIEW, "my_fv", "v1",
                    primary_keys=["user_id"],
                    partition_columns=["dt"],
                    storage_base_path="gs://bucket/features/my_fv/v1")
```

**Auto-inference (quick path):**
```python
client.write_entity(df, EntityKind.FEATURE_VIEW, "my_fv", "v1")
# infers: primary_keys=["id"], partition_columns=["dt"],
#         storage_base_path="{registry_parent}/feature_views/my_fv/v1"
```

**Opt-out (strict mode):**
```python
client.write_entity(df, EntityKind.FEATURE_VIEW, "my_fv", "v1",
                    auto_register=False)
# raises EntityNotFoundError if not pre-registered
```

#### Backward compatibility

- `auto_register` defaults to `True` — existing callers that didn't pre-register will now auto-register instead of erroring
- Callers that do pre-register are unaffected (path 2 in flow — the config is found, no auto-register needed)
- `auto_register=False` restores the original strict behavior
