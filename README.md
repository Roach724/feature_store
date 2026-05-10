# Feature Store Client

A PySpark-native Feature Store client with YAML-based registry, supporting
feature views, model feature sets, labels, datasets, and training sets.

## Core Capabilities

- **YAML Registry** — unified `kind`-based YAML format for all entity types
- **Five Entity Types** — feature_view, model, label, dataset, training_set
- **Pluggable Storage** — GCS and local backends, extensible to S3
- **Spark Lineage Checkpoint** — context-managed checkpoint with UUID isolation
  for parallel writes
- **Auto-register on Write** — first write automatically registers entity
  metadata
- **Dependency DAG** — recursive BFS traversal over entity dependencies
- **Lifecycle Management** — sync TTL/cold-tier rules from registry to GCS
  buckets
- **Migration Tool** — convert legacy YAML format to current schema

## Installation

```bash
# From source
pip install -e ".[dev]"

# From wheel (for Spark cluster)
pip install dist/feature_store-2.0.0-py3-none-any.whl

# As --py-files (for spark-submit)
spark-submit --py-files dist/feature_store.zip ...
```

## Quick Start

```python
from pyspark.sql import SparkSession
from feature_store import FeatureStoreClient, EntityKind

spark = SparkSession.builder.appName("demo").getOrCreate()
client = FeatureStoreClient(spark, registry_dir="gs://bucket/registry")

# Register and write a feature view in one step
df = spark.createDataFrame(
    [(1, "2024-01-01", 25, 0.8)],
    ["user_id", "dt", "age", "score"]
)
client.write_entity(
    df, EntityKind.FEATURE_VIEW, "user_features", "v1",
    primary_keys=["user_id"],
    partition_columns=["dt"],
    storage_base_path="gs://bucket/features/user_features/v1",
)

# Read features
features = client.get_entity(
    EntityKind.FEATURE_VIEW, "user_features", "v1",
    columns=["age", "score"],
    start_date="2024-01-01",
)
features.show()
```

## Documentation

| Document | Description |
|----------|-------------|
| [API Reference](docs/API.md) | Module overview and full client API reference |
| [CI/CD](docs/CI_CD.md) | Continuous integration and deployment pipeline |
| [Changelog](CHANGELOG.md) | Version history and release notes |
| [Design Specs](docs/superpowers/specs/) | Design documents for each feature |

## License

Internal use.
