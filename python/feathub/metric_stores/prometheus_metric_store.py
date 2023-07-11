#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
from typing import Any, Dict, List, Optional, OrderedDict

from feathub.common.config import ConfigDef
from feathub.common.utils import generate_random_name
from feathub.feature_tables.sinks.prometheus_sink import PrometheusSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
from feathub.feature_views.transforms.expression_transform import ExpressionTransform
from feathub.metric_stores.metric import Metric
from feathub.metric_stores.metric_store import MetricStore
from feathub.metric_stores.metric_store_config import (
    METRIC_STORE_PREFIX,
)
from feathub.metric_stores.metric_store_config import (
    MetricStoreConfig,
)
from feathub.processors.materialization_descriptor import MaterializationDescriptor
from feathub.table.table_descriptor import TableDescriptor

PROMETHEUS_METRIC_STORE_PREFIX = METRIC_STORE_PREFIX + "prometheus."

SERVER_URL_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "server_url"
SERVER_URL_DOC = "The PushGateway server URL including scheme, host name, and port."

DELETE_ON_SHUTDOWN_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "delete_on_shutdown"
DELETE_ON_SHUTDOWN_DOC = """
Whether to delete metrics from Prometheus when the job finishes. When set to true,
Feathub will try its best to delete the metrics but this is not guaranteed.
"""


prometheus_metric_store_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=SERVER_URL_CONFIG,
        value_type=str,
        description=SERVER_URL_DOC,
    ),
    ConfigDef(
        name=DELETE_ON_SHUTDOWN_CONFIG,
        value_type=bool,
        description=DELETE_ON_SHUTDOWN_DOC,
        default_value=True,
    ),
]


class PrometheusMetricStoreConfig(MetricStoreConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(prometheus_metric_store_config_defs)


# TODO: Allow users to configure feature name pattern.
class PrometheusMetricStore(MetricStore):
    """
    A metric store reporting metrics to Prometheus through push gateway. For user-defined
    Metrics, they would be reported in the following format:

    - metric name: "{namespace}_{feature_name}_{metric_type}".
    - tags: the metric tags along with the following tags.
        - feature_name: The name of the host feature
        - table_name: The name of the sink where the host features would be written to.
        - job: The namespace of the metric store.
    """

    def __init__(self, props: Dict) -> None:
        """
        :param props: The metric store properties.
        """
        super().__init__(props)
        prometheus_metric_store_config = PrometheusMetricStoreConfig(props)
        self.server_url = prometheus_metric_store_config.get(SERVER_URL_CONFIG)
        self.delete_on_shutdown = prometheus_metric_store_config.get(
            DELETE_ON_SHUTDOWN_CONFIG
        )

    def materialize_metrics_if_any(
        self,
        feature_descriptor: TableDescriptor,
        data_sink: Sink,
    ) -> Optional[MaterializationDescriptor]:
        if not any(
            feature.metrics for feature in feature_descriptor.get_output_features()
        ):
            return None
        return MaterializationDescriptor(
            feature_descriptor=self._get_metrics_view(feature_descriptor, data_sink),
            sink=self._get_metrics_sink(data_sink),
            allow_overwrite=True,
        )

    def _get_metric_name(self, metric: Metric, feature: Feature) -> str:
        return f"{self.namespace}_{feature.name}_{metric.metric_type}"

    def _get_metric_tags(
        self, metric: Metric, feature: Feature, data_sink: Sink
    ) -> OrderedDict[str, str]:
        tags = collections.OrderedDict([
            ("feature_name", feature.name),
            ("table_name", data_sink.name),
        ])
        for key, value in metric.get_tags().items():
            tags[key] = value
        return tags

    # TODO: support treating zero window as infinite window in SlidingWindowTransform
    def _get_metrics_view(
        self, features_desc: TableDescriptor, data_sink: Sink
    ) -> TableDescriptor:
        metric_name_count_map = dict()
        for feature in features_desc.get_output_features():
            for metric in feature.metrics:
                metric_name = self._get_metric_name(metric, feature)
                metric_name_count_map[metric_name] = metric_name_count_map.get(metric_name, 0) + 1
        metric_name_map = dict()
        for metric_name, count in metric_name_count_map.items():
            if count > 1:
                metric_name_map[metric_name] = []

        metric_features = []
        for feature in features_desc.get_output_features():
            for metric in feature.metrics:
                previous_metric_name = feature.name
                for transform_function in metric.get_transformations():
                    tmp_metric_name = generate_random_name("feathub_metric")
                    transform = transform_function(previous_metric_name)
                    metric_features.append(Feature(
                        name=tmp_metric_name,
                        transform=transform,
                    ))
                    previous_metric_name = tmp_metric_name
                metric_name = self._get_metric_name(metric, feature)
                metric_tags = self._get_metric_tags(metric, feature, data_sink)
                metric_tags_str = ",".join(f"{k}={v}" for k, v in metric_tags.items())
                if metric_name in metric_name_map:
                    metric_name_map[metric_name].append((previous_metric_name, metric_tags_str))
                else:
                    metric_features.append(Feature(
                        name=metric_name,
                        transform=f"`{previous_metric_name}`",
                        description=metric_tags_str
                    ))

        for metric_name, tmp_metric_names_and_tags in metric_name_map.items():
            metric_features.append(Feature(
                name=metric_name,
                transform=f"ARRAY({','.join(x[0] for x in tmp_metric_names_and_tags)})",
                description=";".join(x[1] for x in tmp_metric_names_and_tags)
            ))

        sliding_feature_view = SlidingFeatureView(
            name=f"{features_desc.name}_metrics",
            source=features_desc,
            features=metric_features,
        )
        for metric_name in metric_name_count_map.keys():
            print(metric_name)

        return DerivedFeatureView(
            name=f"{features_desc.name}_filtered_metrics",
            source=sliding_feature_view,
            features=[
                Feature(name=metric_name, transform=f"`{metric_name}`") for metric_name in metric_name_count_map.keys()
            ],
            keep_source_fields=False,
        )

    def _get_metrics_sink(self, data_sink: Sink) -> Sink:
        return PrometheusSink(
            server_url=self.server_url,
            namespace=self.namespace,
            delete_on_shutdown=self.delete_on_shutdown,
            extra_labels={"table_name": data_sink.name},
        )
