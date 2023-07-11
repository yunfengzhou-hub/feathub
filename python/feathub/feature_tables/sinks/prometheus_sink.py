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
        delete_on_shutdown: bool = True,
        extra_labels: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(
            name="",
            system_name="prometheus",
            table_uri={
                "host_url": host_url,
            },
        )
        self.host_url = host_url
        self.delete_on_shutdown = delete_on_shutdown
        self.extra_labels = extra_labels if extra_labels is not None else {}

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "host_url": self.host_url,
            "delete_on_shutdown": self.delete_on_shutdown,
            "extra_labels": self.extra_labels,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "PrometheusSink":
        return PrometheusSink(
            host_url=json_dict["host_url"],
            delete_on_shutdown=json_dict["delete_on_shutdown"],
            extra_labels=json_dict["extra_labels"],
        )
