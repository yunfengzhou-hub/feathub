#  Copyright 2022 The Feathub Authors
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
from datetime import timedelta
from typing import Optional, List, Sequence, Dict

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.sink import Sink
from feathub.table.schema import Schema


class HBaseSink(Sink):
    """A source that reads data from HBase."""
    def __init__(
        self,
        name: str,
        table_name: str,
        zk_quorum: str,
    ):
        super().__init__(
            name=name,
            system_name="hbase",
            properties={"table_name": table_name, "zk_quorum": zk_quorum},
        )

        self.table_name = table_name
        self.zk_quorum = zk_quorum

    def to_json(self) -> Dict:
        pass
        # return {
        #     "type": "RedisSource",
        #     "name": self.name,
        #     "schema": None if self.schema is None else self.schema.to_json(),
        #     "keys": self.keys,
        #     "timestamp_field": self.timestamp_field,
        # }

