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
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Dict, Optional

from feathub.feature_tables.sinks.sink import Sink
from feathub.metric_stores.metric_store_config import (
    MetricStoreConfig,
    MetricStoreType,
    METRIC_STORE_TYPE_CONFIG,
    METRIC_STORE_NAMESPACE_CONFIG,
    METRIC_STORE_REPORT_INTERVAL_CONFIG,
)
from feathub.processors.materialization_descriptor import MaterializationDescriptor
from feathub.table.table_descriptor import TableDescriptor


class MetricStore(ABC):
    """
    A MetricStore provides properties to set the metrics of a Feathub job
    into an external metric service.
    """

    def __init__(self, config: Dict) -> None:
        """
        :param config: The metric store configuration.
        """
        self._metric_store_config = MetricStoreConfig(config)

    @staticmethod
    def instantiate(props: Dict) -> Optional["MetricStore"]:
        """
        Instantiates a metric store using the given properties.
        """

        # TODO: remove code below after local metric store is introduced as
        #  default value.
        if METRIC_STORE_TYPE_CONFIG not in props:
            return None

        metric_store_config = MetricStoreConfig(props)
        metric_store_type = MetricStoreType(
            metric_store_config.get(METRIC_STORE_TYPE_CONFIG)
        )

        if metric_store_type == MetricStoreType.PROMETHEUS:
            from feathub.metric_stores.prometheus_metric_store import (
                PrometheusMetricStore,
            )

            return PrometheusMetricStore(props=props)

        raise RuntimeError(f"Failed to instantiate metric store with props={props}.")

    @property
    def namespace(self) -> str:
        return self._metric_store_config.get(METRIC_STORE_NAMESPACE_CONFIG)

    @property
    def report_interval(self) -> timedelta:
        return timedelta(
            seconds=self._metric_store_config.get(METRIC_STORE_REPORT_INTERVAL_CONFIG)
        )

    @abstractmethod
    def materialize_metrics_if_any(
        self,
        feature_descriptor: TableDescriptor,
        data_sink: Sink,
    ) -> Optional[MaterializationDescriptor]:
        """
        Creates a materialization descriptor used to materialize metrics in the features
        of a table descriptor to this metric store, if there are any metrics attached to
        the features.

        :param feature_descriptor: The descriptor that might contain metrics to be
                                   materialized.
        :param data_sink: The sink where the feature values will be written to.
        """
        pass
