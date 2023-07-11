#  Copyright 2022 The FeatHub Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import glob
import os

from pyflink.table import (
    expressions as native_flink_expr,
    TableDescriptor as NativeFlinkTableDescriptor,
    StatementSet,
    Schema as NativeFlinkSchema,
    Table as NativeFlinkTable,
    StreamTableEnvironment,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.prometheus_sink import PrometheusSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.metric_stores.prometheus_metric_store import PrometheusMetricStore
from feathub.processors.flink.flink_jar_utils import add_jar_to_t_env, find_jar_lib
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    generate_random_table_name,
)
from feathub.table.table_descriptor import TableDescriptor


def get_prometheus_metrics_sink(metric_store: PrometheusMetricStore, sink: Sink) -> Sink:
    return PrometheusSink(
        host_url=metric_store.host_url,
        delete_on_shutdown=metric_store.delete_on_shutdown,
        extra_labels={"table_name": sink.name}
    )


def add_prometheus_sink_to_statement_set(
    t_env: StreamTableEnvironment,
    statement_set: StatementSet,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: PrometheusSink,
) -> None:
    add_jar_to_t_env(t_env, _get_prometheus_connector_jar())

    features_table = features_table.drop_columns(
        native_flink_expr.col(features_desc.timestamp_field)
    )

    schema = features_table.get_schema()
    builder = NativeFlinkSchema.new_builder()
    for field_name in schema.get_field_names():
        builder.column(field_name, schema.get_field_data_type(field_name))
        description = features_desc.get_feature(field_name).description
        if description:
            builder._j_builder.withComment(description)

    flink_schema = builder.build()

    descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector("prometheus")
        .schema(flink_schema)
        .option("hostUrl", sink.host_url)
        .option("jobName", generate_random_table_name("feathub_metrics"))
        .option("deleteOnShutdown", str(sink.delete_on_shutdown))
        .option(
            "extraLabels",
            ";".join(f"{key}={value}" for key, value in sink.extra_labels.items()),
        )
    )

    statement_set.add_insert(descriptor_builder.build(), features_table)


def _get_prometheus_connector_jar() -> str:
    lib_dir = find_jar_lib()
    jars = glob.glob(os.path.join(lib_dir, "flink-connector-prometheus-*.jar"))
    if len(jars) < 1:
        raise FeathubException(
            f"Can not find the Flink Prometheus connector jar at {lib_dir}."
        )
    return jars[0]
