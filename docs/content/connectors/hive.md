# Hive

FeatHub provides `HiveSource` to read data from a Hive table and `HiveSink` to
materialize feature view to a Hive table.

## Supported Processors and Modes

- Flink: Streaming Scan, Streaming Append

## Examples

Here are the examples of using `HiveSource` and `HiveSink`:

### Use as Streaming Append Sink

```python
feature_view = DerivedFeatureView(...)

hive_sink = HiveSink(
    database="default",
    hive_conf_dir=".",
    extra_config={
        'sink.partition-commit.policy.kind': 'metastore,success-file',
    }
)

feathub_client.materialize_features(
    features=feature_view,
    sink=sink,
    allow_overwrite=True,
).wait(30000)
```

### Use as Streaming Scan Source

```python
schema = (
    Schema.new_builder()
    ...
    .build()
)

source = HiveSource(
    name="hive_source",
    database="default",
    table="table",
    schema=schema,
    keys=["key"],
    hive_conf_dir=".",
    timestamp_field="timestamp",
    timestamp_format="%Y-%m-%d %H:%M:%S %z",
)

feature_view = DerivedFeatureView(
    name="feature_view",
    source=source,
    features=[
        ...
    ],
)
```