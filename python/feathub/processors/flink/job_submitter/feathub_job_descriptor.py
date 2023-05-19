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
from datetime import datetime
from typing import Optional, Dict, Any, Union

import pandas as pd

from feathub.common.utils import from_json, append_metadata_to_json
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.table_descriptor import TableDescriptor


class FeathubJobDescriptor:
    """Descriptor of a FeatHub job to run in a remote Flink cluster."""

    def __init__(
        self,
        features: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
        sink: FeatureTable,
        local_registry_tables: Dict[str, TableDescriptor],
        allow_overwrite: bool,
        props: Dict,
    ):
        """
        Instantiate a FeathubJobDescriptor.

        :param features: The table descriptor that contains the features to compute.
        :param keys: Optional. If it is TableDescriptor or DataFrame, it should be
                     transformed into a table of keys. If it is not None, the
                     table only include rows whose key fields match at least one
                     row of the keys.
        :param start_datetime: Optional. If it is not None, the `features` table should
                               have a timestamp field. And the table will only
                               include features whose
                               timestamp >= start_datetime. If any field (e.g. minute)
                               is not specified in the start_datetime, we assume this
                               field has the minimum possible value.
        :param end_datetime: Optional. If it is not None, the `features` table should
                             have a timestamp field. And the table will only
                             include features whose timestamp < end_datetime. If any
                             field (e.g. minute) is not specified in the end_datetime,
                             we assume this field has the maximum possible value.
        :param sink: Where the features write to.
        :param local_registry_tables: All the table descriptors registered in the local
                                      registry that are required to compute the given
                                      table.
        :param allow_overwrite: If it is false, throw error if the features collide with
                                existing data in the given sink.
        :param props: All properties of FeatHub.
        """
        self.features = features
        self.keys = keys
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.sink = sink
        self.local_registry_tables = local_registry_tables
        self.allow_overwrite = allow_overwrite
        self.props = props
        self.to_json = append_metadata_to_json(  # type: ignore
            self.to_json,
            self.__class__,
        )

    def to_json(self) -> Dict:
        return {
            "features": self.features.to_json(),
            "keys": None
            if self.keys is None
            else self.keys.to_json()
            if isinstance(self.keys, TableDescriptor)
            else self.keys.to_json(),
            "start_datetime_ms": None
            if self.start_datetime is None
            else int(self.start_datetime.timestamp() * 1000),
            "end_datetime_ms": None
            if self.end_datetime is None
            else int(self.end_datetime.timestamp() * 1000),
            "sink": self.sink.to_json(),
            "local_registry_tables": {
                k: v if isinstance(v, str) else v.to_json()
                for k, v in self.local_registry_tables.items()
            },
            "allow_overwrite": self.allow_overwrite,
            "props": self.props,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "FeathubJobDescriptor":
        return FeathubJobDescriptor(
            features=from_json(json_dict["features"]),
            keys=None
            if json_dict["keys"] is None
            else pd.read_json(json_dict["keys"])
            if isinstance(json_dict["keys"], str)
            else from_json(json_dict["keys"]),
            start_datetime=None
            if json_dict["start_datetime_ms"] is None
            else datetime.fromtimestamp(json_dict["start_datetime_ms"] / 1000.0),
            end_datetime=None
            if json_dict["end_datetime_ms"] is None
            else datetime.fromtimestamp(json_dict["end_datetime_ms"] / 1000.0),
            sink=from_json(json_dict["sink"]),
            local_registry_tables={
                k: v if isinstance(v, str) else from_json(v)
                for k, v in json_dict["local_registry_tables"].items()
            },
            allow_overwrite=json_dict["allow_overwrite"],
            props=json_dict["props"],
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, FeathubJobDescriptor)
            and self.features == other.features
            and self.keys == other.keys
            and self.start_datetime == other.start_datetime
            and self.end_datetime == other.end_datetime
            and self.sink == other.sink
            and self.local_registry_tables == other.local_registry_tables
            and self.allow_overwrite == other.allow_overwrite
            and self.props == other.props
        )
