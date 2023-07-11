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
import json
import typing

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.metric_stores.metric import Metric, Count, Ratio
from feathub.metric_stores.metric_store import MetricStore
from feathub.metric_stores.prometheus_metric_store import PrometheusMetricStore
from feathub.processors.flink.table_builder.prometheus_utils import (
    get_prometheus_metrics_sink,
)
from feathub.table.table_descriptor import TableDescriptor


def get_metric_tags(
    metric: Metric, feature: Feature
) -> typing.OrderedDict[str, str]:
    tags = collections.OrderedDict(
        [
            ("feature_name", feature.name),
            ("window_time", str(metric.window_size)),
        ]
    )
    if isinstance(metric, Count) or isinstance(metric, Ratio):
        tags["filter_expr"] = metric.filter_expr
    return tags


# TODO: support treating zero window as infinite window in SlidingWindowTransform
def get_metrics_view(
    features_desc: TableDescriptor, metric_store: MetricStore
) -> TableDescriptor:
    if not isinstance(metric_store, PrometheusMetricStore):
        raise FeathubException(f"Unsupported metric store type {type(metric_store)}.")

    metric_features = []
    post_process_metric_features = []
    for feature in features_desc.get_output_features():
        for metric in feature.metrics:
            metric_name = f"feathub_{metric_store.namespace}_feature_{metric.metric_type}"
            metric_tags = get_metric_tags(metric, feature)
            if isinstance(metric, Count):
                metric_features.append(
                    Feature(
                        name=metric_name,
                        transform=SlidingWindowTransform(
                            expr="1",
                            agg_func="COUNT",
                            window_size=metric.window_size,
                            step_size=metric_store.report_interval,
                        ),
                    )
                )
                post_process_metric_features.append(
                    Feature(
                        name=metric_name,
                        transform=f"`{metric_name}`",
                        description=json.dumps(metric_tags),
                    ),
                )
            elif isinstance(metric, Ratio):
                metric_features.append(
                    Feature(
                        name=metric_name,
                        transform=SlidingWindowTransform(
                            expr=f"CASE WHEN {feature.name} {metric.filter_expr} THEN 1 ELSE 0 END",
                            agg_func="AVG",
                            window_size=metric.window_size,
                            step_size=metric_store.report_interval,
                        ),
                    ),
                )
                post_process_metric_features.append(
                    Feature(
                        name=metric_name,
                        transform=f"CASE WHEN `{metric_name}` IS NULL THEN 0 ELSE `{metric_name}` END",
                        description=json.dumps(metric_tags),
                    )
                )

    sliding_feature_view = SlidingFeatureView(
        name=f"{features_desc.name}_metrics",
        source=features_desc,
        features=metric_features,
    )

    return DerivedFeatureView(
        name=f"{features_desc.name}_metrics_postprocess",
        source=sliding_feature_view,
        features=post_process_metric_features,
        keep_source_fields=False,
    )


def get_metrics_sink(metric_store: MetricStore, sink: Sink) -> Sink:
    """
    :param sink: The sink descriptor that the host features of the metrics will be
                 written into.
    """
    if isinstance(metric_store, PrometheusMetricStore):
        return get_prometheus_metrics_sink(metric_store, sink)
    else:
        raise FeathubException(f"Unsupported metric store type {type(metric_store)}.")
