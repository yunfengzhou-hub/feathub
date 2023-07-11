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
import collections
import typing

from pyflink.table import (
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    StatementSet,
)

from feathub.common.exceptions import FeathubException
from feathub.common.utils import get_str_of_time_span
from feathub.feature_tables.sinks.prometheus_sink import PrometheusSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.metric_stores.metric import Metric, Count, Ratio
from feathub.metric_stores.prometheus_metric_store import PrometheusMetricStore


def get_metric_tags(
    metric: Metric, feature: Feature, sink: Sink, metric_store: PrometheusMetricStore
) -> typing.OrderedDict[str, str]:
    tags = collections.OrderedDict(
        [
            ("feature_name", feature.name),
            ("window_time", get_str_of_time_span(metric.window_size)),
            ("table_name", sink.name),
        ]
    )
    if isinstance(metric, Count) or isinstance(metric, Ratio):
        tags["filter_expr"] = metric.filter_expr
    return tags


def get_metric_feature(
    metric: Metric, feature: Feature, metric_store: PrometheusMetricStore
) -> Feature:
    feature_name = f"feathub_{metric_store.namespace}_feature_{metric.metric_type}"
    if isinstance(metric, Count):
        return Feature(
            name=feature_name,
            transform=SlidingWindowTransform(
                expr="1",
                agg_func="COUNT",
                window_size=metric.window_size,
                step_size=metric_store.report_interval,
            ),
        )
    elif isinstance(metric, Ratio):
        return Feature(
            name=feature_name,
            transform=SlidingWindowTransform(
                expr=f"CASE WHEN {feature.name} {metric.filter_expr} THEN 1 ELSE 0 END",
                agg_func="AVG",
                window_size=metric.window_size,
                step_size=metric_store.report_interval,
            ),
        )
    raise FeathubException()


def get_prometheus_metrics_sink(metric_store: PrometheusMetricStore) -> Sink:
    return PrometheusSink(host_url=metric_store.host_url)


def add_prometheus_sink_to_statement_set(
    statement_set: StatementSet,
    features_table: NativeFlinkTable,
    sink: PrometheusSink,
) -> None:
    descriptor_builder = NativeFlinkTableDescriptor.for_connector("prometheus").option(
        "host_url", sink.host_url
    )

    statement_set.add_insert(descriptor_builder.build(), features_table)
