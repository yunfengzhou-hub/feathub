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

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.metric_stores.metric_store import MetricStore
from feathub.metric_stores.prometheus_metric_store import PrometheusMetricStore
from feathub.processors.flink.table_builder.prometheus_utils import (
    get_prometheus_metrics_sink,
)
from feathub.table.table_descriptor import TableDescriptor


def get_metrics_view(features_desc: TableDescriptor) -> TableDescriptor:
    metric_features = []
    for feature in features_desc.get_output_features():
        for metric in feature.metrics:
            metric_features.append(
                Feature(
                    name=metric.name,
                    transform=metric.transform,
                )
            )

    return DerivedFeatureView(
        name=f"{features_desc.name}_metrics",
        source=features_desc,
        features=metric_features,
        keep_source_fields=False,
    )


def get_metrics_sink(metric_store: MetricStore) -> Sink:
    if isinstance(metric_store, PrometheusMetricStore):
        return get_prometheus_metrics_sink(metric_store)
    else:
        raise FeathubException(f"Unsupported metric store type {type(metric_store)}.")
