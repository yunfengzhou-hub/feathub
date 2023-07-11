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
from typing import Dict, Optional

from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.sinks.sink import Sink


class PrometheusSink(Sink):
    def __init__(
        self,
        host_url: str,
        namespace: str = "default",
        delete_on_shutdown: bool = True,
        grouping_keys: Optional[Dict[str, str]] = None,
    ):
        """
        :param host_url: The PushGateway server host URL including scheme,
                         host name, and port.
        :param namespace: The namespace to report metrics to Prometheus. Metrics
                          within different namespace will not overwrite each other.
        :param delete_on_shutdown: Whether to delete metrics from Prometheus
                                   when the job finishes. When set to true,
                                   Feathub will try its best to delete the
                                   metrics but this is not guaranteed.
        :param grouping_keys: The grouping keys which are the group and global
                              labels of all metrics pushed by this sink.
        """
        super().__init__(
            name="",
            system_name="prometheus",
            table_uri={
                "host_url": host_url,
            },
        )
        self.host_url = host_url
        self.namespace = namespace
        self.delete_on_shutdown = delete_on_shutdown
        self.grouping_keys = grouping_keys if grouping_keys is not None else {}

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "host_url": self.host_url,
            "namespace": self.namespace,
            "delete_on_shutdown": self.delete_on_shutdown,
            "grouping_keys": self.grouping_keys,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PrometheusSink":
        return PrometheusSink(
            host_url=json_dict["host_url"],
            namespace=json_dict["namespace"],
            delete_on_shutdown=json_dict["delete_on_shutdown"],
            grouping_keys=json_dict["grouping_keys"],
        )
