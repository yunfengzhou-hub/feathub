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

from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_tables.sources.hive_source import get_hive_catalog_identifier


# TODO: Support SQL grammars like `INSERT OVERWRITE` and `PARTITION` in
#  Feathub connectors.
class HiveSink(Sink):
    """
    A sink that write data to Hive.
    """

    def __init__(
        self,
        database: str,
        table: str,
        hive_catalog_conf_dir: str,
        processor_specific_props: Optional[Dict[str, str]] = None,
    ):
        """
        :param database: The database to write to.
        :param table: Table name of the table to write to.
        :param hive_catalog_conf_dir: URI to your Hive conf dir containing
                                      hive-site.xml. The configuration would be used
                                      to create the Hive Catalog. The URI needs to be
                                      supported by Hadoop FileSystem. If the URI is
                                      relative, i.e. without a scheme, local file
                                      system is assumed.
        :param processor_specific_props: Extra properties to be passthrough to the
                                         processor. The available configurations are
                                         different for different processors.
        """
        super().__init__(
            name="",
            system_name="hive",
            table_uri={
                "hive_catalog_identifier": get_hive_catalog_identifier(
                    hive_catalog_conf_dir
                ),
                "database": database,
            },
        )
        self.database = database
        self.table = table
        self.hive_catalog_conf_dir = hive_catalog_conf_dir
        self.processor_specific_props = processor_specific_props

    def to_json(self) -> Dict:
        return {
            "type": "HiveSink",
            "database": self.database,
            "table": self.table,
            "hive_catalog_conf_dir": self.hive_catalog_conf_dir,
            "processor_specific_props": self.processor_specific_props,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "HiveSink":
        return HiveSink(
            database=json_dict["database"],
            table=json_dict["table"],
            hive_catalog_conf_dir=json_dict["hive_catalog_conf_dir"],
            processor_specific_props=json_dict["processor_specific_props"],
        )
