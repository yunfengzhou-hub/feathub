# Feathub Architecture and Concepts

<img src="./figures/architecture_1.png" width="50%" height="auto">

The figure above shows the key components and concepts in the FeatHub
architecture.

<img src="./figures/architecture_2.png" width="70%" height="auto">

The figure above shows how a developer can use FeatHub to facilitate feature
engineering for model training and model inference. We describe the key concepts
in the FeatHub architecture below.

## TableDescriptor - Declarative Definition of Features

A `TableDescriptor` provides metadata to acces, derive and interpret a
table of feature values. Each column of the table corresponds to a feature.

A table in Feathub is conceptually similar to a table in Apache Flink, with
first-class support for timestamp column. If a timestamp column is specified, it
is guaranteed that all feature values of a row is available at the time
specified by this column. This column is necessary to perform point-in-time correct
table join.

`TableDescriptor` has the following sub-classes.

### Source

A `Source` provides metadata to access and interpret a table of feature
values from an offline or online feature store. For example, `FileSystemSource`
can specify the path of a file containing feature values in csv format.

### Sink

A `Sink` provides metadata to locate and write a table of feature values to an
offline or online feature store.

### FeatureView

A `FeatureView` provides metadata to derive a table of feature values from
other tables. Feathub currently supports the following types of FeatureViews.

- `DerivedFeatureView` derives features by applying the given transformations on
  an existing table. It supports per-row transformation, over window transformation 
  and table join. It does not support sliding window transformation.
- `OnDemandFeatureView` derives features by joining online request with features
  from tables in online feature stores. It supports per-row transformation and
  join with tables in online stores. It does not support over window transformation or 
  sliding window transformation.
- `SlidingFeatureView` derives features by applying the given transformations on an 
  existing table. It supports per-row transformation and sliding window transformation. 
  It does not support join or over window transformation.
- `SqlFeatureView` derives features by evaluating a given SQL statement.
  Currently, its semantics depends on the processor used during deployment. We
  plan to make it processor-agnostic in the future to ensure consistent
  semantics regardless of processor choice.

`FeatureView` provides APIs to specify and access `Feature`s. Each `Feature` is
defined by the following metadata:
- `name`: a string that uniquely identifies this feature in the parent table.
- `dtype`: the data type of this feature's values.
- `transform`: A declarative definition of how to derive this feature's values.
- `keys`: an optional list of strings, corresponding to the names of fields in
  the parent table necessary to interpret this feature's values. If it is
  specified, it is used as the join key when Feathub joins this feature onto
  another table.

## Transformation - Declarative Definition of Feature Computation

A `Transformation` defines how to derive a new feature from existing features.
Feathub currently supports the following types of Transformations.

- `ExpressionTransform` derives feature values by applying Feathub expression on
  one row of the parent table at a time. Feathub expression language is a
  declarative language with build-in functions. See [Feathub
  expression](feathub_expression.md) for more information.
- `OverWindowTransform` derives feature values by applying Feathub expression and
  aggregation function on multiple rows of a table at a time.
- `SlidingWindowTransform` derives feature values by applying Feathub expression and 
  aggregation function on multiple rows in a sliding window.
- `JoinTransform` derives feature values by joining parent table with a feature
  from another table.
- `PythonUdfTransform` derives feature values by applying a Python UDF on one
  row of the parent table at a time.



## Processor - Pluggable Compute Engine for Feature Generation

A `Processor` is a pluggable compute engine that implements APIs to extract,
transform, and load feature values into feature stores. A ``Processor is
responsible to recognize declarative specifications of `Transformation`,
`Source` and `Sink`, and compile them into the corresponding jobs (e.g. Flink
jobs) for execution.

Feathub currently supports `LocalProcessor`, which uses CPUs on the local
machine to compute features, with Pandas DataFrame as the underlying
representation of tables. This processor allows data scientists to run
experiments on a single machine, without relying on remote clusters, when the
storage and computation capability on a single machine is sufficient.

As the next step, we plan to support `FlinkProcessor`, which starts Flink jobs
to extract, compute and load features into feature stores, with Flink table as
the underlying representation of tables. This processor allows data scientists
to run feature generation jobs with scalability and fault tolerance on a
distributed Flink cluster.

Users should be able to switch between processors by simply specifying the
processor type in the `FeathubClient` configuration, without having to change
any code related to the feature generation. This allows Feathub to maximize
developer velocity by providing data scientists with a smooth self-serving
experiment-to-production experience.


## Feature Registry

A registry implements APIs to build, register, get, and delete table
descriptors, such as feature views with feature transformation definitions. It
improves developer velocity by allowing different teams in an organization to
collaborate, share, discover, and re-use features.

Feathub currently supports `LocalRegistry`, which persists feature definitions
on local filesystem.

In the future, we can implement additional registry to integrate Feathub with
existing metadata platform such as
[DataHub](https://github.com/datahub-project/datahub).

## OnlineStore

An online store implements APIs to put and get features by keys. It can provide a
uniform interface to interact with kv stores such as BigQuery and Redis.

## Feature Service

A FeatureService implements APIs to compute on-demand feature view, which
involves joining online request with features from tables in online stores, and
performing per-row transformation after online request arrives.

Unlike Processor, which computes features with offline or nearline latency,
FeatureService computes features with online latency immediately after online
request arrives. And it can derive new features based on the values in the
user's request.

