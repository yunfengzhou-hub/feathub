# Copyright 2022 The FeatHub Authors
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
from abc import ABC
from datetime import timedelta
from typing import Dict, Optional

from feathub.metric_stores.metric_store_config import (
    MetricStoreConfig,
    MetricStoreType,
    METRIC_STORE_TYPE_CONFIG,
    METRIC_STORE_NAMESPACE_CONFIG,
    METRIC_STORE_REPORT_INTERVAL_CONFIG,
)


class MetricStore(ABC):
    """
    foobar.
    """

    def __init__(self, metric_store_type: str, config: Dict) -> None:
        """
        :param metric_store_type: The type of the registry
        :param config: The registry configuration.
        """
        self._metric_store_config = MetricStoreConfig(config)
        self._metric_store_type = metric_store_type
        self._config = config

    @staticmethod
    def instantiate(props: Dict) -> Optional["MetricStore"]:
        """
        Instantiates a registry using the given properties.
        """

        if METRIC_STORE_TYPE_CONFIG not in props:
            return None

        metric_store_config = MetricStoreConfig(props)
        metric_store_type = MetricStoreType(
            metric_store_config.get(METRIC_STORE_TYPE_CONFIG)
        )

        if metric_store_type is None:
            return None

        if metric_store_type == MetricStoreType.PROMETHEUS:
            from feathub.metric_stores.prometheus_metric_store import (
                PrometheusMetricStore,
            )

            return PrometheusMetricStore(props=props)

        raise RuntimeError(f"Failed to instantiate metric store with props={props}.")

    @property
    def config(self) -> Dict:
        return self._config

    @property
    def metric_store_type(self) -> str:
        return self._metric_store_type

    @property
    def namespace(self) -> str:
        return self._metric_store_config.get(METRIC_STORE_NAMESPACE_CONFIG)

    @property
    def report_interval(self) -> timedelta:
        return self._metric_store_config.get(METRIC_STORE_REPORT_INTERVAL_CONFIG)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self._config == other._config
            and self._metric_store_type == other._metric_store_type
        )
