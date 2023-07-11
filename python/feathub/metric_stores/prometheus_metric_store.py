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
import json
from typing import Any, Dict, List, Optional

from feathub.common.config import ConfigDef
from feathub.common.utils import generate_random_name
from feathub.feature_tables.sinks.prometheus_sink import PrometheusSink
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
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

HOST_URL_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "host_url"
HOST_URL_DOC = "The PushGateway server host URL including scheme, host name, and port."

DELETE_ON_SHUTDOWN_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "delete_on_shutdown"
DELETE_ON_SHUTDOWN_DOC = """
Whether to delete metrics from Prometheus when the job finishes. When set to true,
Feathub will try its best to delete the metrics but this is not guaranteed.
"""


prometheus_metric_store_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=HOST_URL_CONFIG,
        value_type=str,
        description=HOST_URL_DOC,
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


class PrometheusMetricStore(MetricStore):
    """
    A metric store reporting metrics to Prometheus through push gateway. Metrics
    would be reported in the following format:

    - metric name: "feathub_{namespace}_{scope}_{metric_type}". For metrics defined
                   on Features, scope is "feature".
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
        self.host_url = prometheus_metric_store_config.get(HOST_URL_CONFIG)
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

    def _get_metric_name(self, metric: Metric) -> str:
        return f"feathub_{self.namespace}_feature_{metric.metric_type}"

    def _get_metric_tags(
        self, metric: Metric, feature: Feature, data_sink: Sink
    ) -> Dict[str, str]:
        return {
            **metric.get_tags(),
            "feature_name": feature.name,
            "table_name": data_sink.name,
        }

    # TODO: support treating zero window as infinite window in SlidingWindowTransform
    def _get_metrics_view(
        self, features_desc: TableDescriptor, data_sink: Sink
    ) -> TableDescriptor:
        metric_features = []
        post_process_metric_features = []
        for feature in features_desc.get_output_features():
            for metric in feature.metrics:
                metric_tags = self._get_metric_tags(metric, feature, data_sink)
                metric_tags["metric_name"] = self._get_metric_name(metric)
                metric_feature_name = generate_random_name("feathub_metric")
                metric_features.append(
                    Feature(
                        name=metric_feature_name,
                        transform=metric.get_agg_transform(
                            feature.name, self.report_interval
                        ),
                    )
                )
                post_process_metric_features.append(
                    Feature(
                        name=metric_feature_name,
                        transform=metric.get_post_agg_transform(metric_feature_name),
                        description=json.dumps(metric_tags),
                    ),
                )

        sliding_feature_view = SlidingFeatureView(
            name=f"{features_desc.name}_metrics",
            source=features_desc,
            features=metric_features,
        )

        return DerivedFeatureView(
            name=f"{features_desc.name}_post_processed_metrics",
            source=sliding_feature_view,
            features=post_process_metric_features,
            keep_source_fields=False,
        )

    def _get_metrics_sink(self, data_sink: Sink) -> Sink:
        return PrometheusSink(
            host_url=self.host_url,
            namespace=self.namespace,
            delete_on_shutdown=self.delete_on_shutdown,
            grouping_keys={"table_name": data_sink.name},
        )
