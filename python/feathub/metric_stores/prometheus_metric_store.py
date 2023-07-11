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
from typing import Any, Dict

from feathub.common.config import ConfigDef
from feathub.metric_stores.metric_store import MetricStore
from feathub.metric_stores.metric_store_config import (
    METRIC_STORE_PREFIX,
    MetricStoreConfig,
    MetricStoreType,
)

PROMETHEUS_METRIC_STORE_PREFIX = METRIC_STORE_PREFIX + "prometheus."

HOST_URL_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "host_url"

DELETE_ON_SHUTDOWN_CONFIG = PROMETHEUS_METRIC_STORE_PREFIX + "delete_on_shutdown"


prometheus_metric_store_config_defs = [
    ConfigDef(
        name=HOST_URL_CONFIG,
        value_type=str,
        description="The PushGateway server host URL including scheme, host name, and port.",
    ),
    ConfigDef(
        name=DELETE_ON_SHUTDOWN_CONFIG,
        value_type=bool,
        description="Whether to delete metrics from Prometheus when the job finishes. When "
        "set to true, Feathub will try its best to delete the metrics but this is "
        "not guaranteed.",
        default_value=True,
    ),
]


class PrometheusMetricStoreConfig(MetricStoreConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(prometheus_metric_store_config_defs)


class PrometheusMetricStore(MetricStore):
    def __init__(self, props: Dict) -> None:
        """
        :param props: The registry properties.
        """
        super().__init__(MetricStoreType.PROMETHEUS.name, props)
        prometheus_metric_store_config = PrometheusMetricStoreConfig(props)
        self.host_url = prometheus_metric_store_config.get(HOST_URL_CONFIG)
        self.delete_on_shutdown = prometheus_metric_store_config.get(
            DELETE_ON_SHUTDOWN_CONFIG
        )
