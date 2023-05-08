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
from typing import Optional, Dict, List

from feathub.feature_tables.feature_table import FeatureTable

from feathub.table.schema import Schema


class HiveSource(FeatureTable):
    """
    A source that reads data from Hive.
    """

    def __init__(
        self,
        name: str,
        database: str,
        table: str,
        schema: Schema,
        keys: Optional[List[str]] = None,
        hive_conf_dir: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        extra_config: Optional[Dict[str, str]] = None,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param database: The database to read from.
        :param table: Table name of the table to read from.
        :param schema: The schema of the table.
        :param hive_conf_dir: URI to your Hive conf dir containing hive-site.xml.
                              The URI needs to be supported by Hadoop FileSystem.
                              If the URI is relative, i.e. without a scheme, local
                              file system is assumed. If the option is not specified,
                              hive-site.xml is searched in class path.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field. See TableDescriptor
                                 for valid format values. Only effective when the
                                 `timestamp_field` is not None.
        :param extra_config: Extra configurations to be passthrough to the processor.
                             The available configurations are different for different
                             processors.
        """
        super(HiveSource, self).__init__(
            name=name,
            system_name="hive",
            table_uri={
                "table": table,
                "hive_conf_dir": hive_conf_dir,
                "database": database,
            },
            keys=keys,
            schema=schema,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.name = name
        self.table = table
        self.schema = schema
        self.hive_conf_dir = hive_conf_dir
        self.database = database
        self.extra_config = extra_config

    def to_json(self) -> Dict:
        return {
            "type": "HiveSource",
            "name": self.name,
            "database": self.database,
            "table": self.table,
            "schema": None if self.schema is None else self.schema.to_json(),
            "keys": self.keys,
            "hive_conf_dir": self.hive_conf_dir,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "extra_config": self.extra_config,
        }
