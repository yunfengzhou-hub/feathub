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
from enum import Enum
from typing import Dict, Optional

from feathub.feature_tables.sinks.sink import Sink


class HBaseType(Enum):
    ALICLOUD = "AliCloud"


class HBaseSink(Sink):
    def __init__(
        self,
        table_name: str,
        zookeeper_quorum: str,
        hbase_type: HBaseType,
        flink_extra_config: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        :param table_name: The name of HBase table to connect.
        :param zookeeper_quorum: The HBase Zookeeper quorum.
        :param hbase_type: The type of the HBase to connect.
        :param flink_extra_config: Extra configuration to the HBaseSink for Flink
                                   processor. Please refer to
                                   https://help.aliyun.com/document_detail/178499.html
                                   for all the available configuration.
        """
        super().__init__(
            name="",
            system_name="hbase",
            properties={
                "table_name": table_name,
                "zookeeper_quorum": zookeeper_quorum,
            }
        )

        self.table_name = table_name
        self.zookeeper_quorum = zookeeper_quorum
        self.hbase_type = hbase_type
        self.flink_extra_config = flink_extra_config

    def to_json(self) -> Dict:
        return {
            "type": "KafkaSink",
            "table_name": self.table_name,
            "zookeeper_quorum": self.zookeeper_quorum,
            "hbase_type": self.hbase_type,
            "flink_extra_config": self.flink_extra_config,
        }
