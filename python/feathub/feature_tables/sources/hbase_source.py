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
from feathub.table.schema import Schema


class HBaseSource(FeatureTable):
    """A source that reads data from HBase."""
    def __init__(
        self,
        name: str,
        table_name: str,
        zk_quorum: str,
        value_format: str,
        schema: Schema,
        column_families: Dict[str, Schema],
        keys: str = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        max_out_of_orderness: timedelta = timedelta(0),
    ):
        super().__init__(
            name=name,
            system_name="kafka",
            properties={"table_name": table_name, "zk_quorum": zk_quorum},
            data_format=value_format,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            schema=schema,
        )

        self.table_name = table_name
        self.zk_quorum = zk_quorum

        for column_family in column_families:
            for column_name in column_family:
                if column_name not in schema.field_names:
                    raise FeathubException(f"Column {column_name} not found in schema.")
        self.column_families = column_families
        self.max_out_of_orderness = max_out_of_orderness

    def to_json(self) -> Dict:
        pass
        # return {
        #     "type": "RedisSource",
        #     "name": self.name,
        #     "schema": None if self.schema is None else self.schema.to_json(),
        #     "keys": self.keys,
        #     "timestamp_field": self.timestamp_field,
        # }

