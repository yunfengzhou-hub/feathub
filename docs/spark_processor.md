# Spark Processor

The SparkProcessor does feature ETL using Spark as the processing engine. In the
following sections we describe the deployment modes supported by SparkProcessor
and the configuration keys accepted by each mode.

## Deployment Mode
FeatHub's SparkProcessor only supports Spark's local mode. When running in local
mode, Feathub will execute the Spark job on the local machine. No Spark cluster
is required.

## Configurations

In the following we describe the configuration keys accepted by the
configuration dict passed to the SparkProcessor.

| key             | Required | default | type   | Description                                                                              |
|-----------------|----------|---------|--------|------------------------------------------------------------------------------------------|
| master | Required | (None) | String | The Spark master URL to connect to. |
| native.*                | optional | (none)         | String | Any key with the "native" prefix will be forwarded to the Spark Session config after the "native" prefix is removed. For example, if the processor config has an entry "native.spark.default.parallelism": 2, then the Spark Session config will have an entry "spark.default.parallelism": 2. |


