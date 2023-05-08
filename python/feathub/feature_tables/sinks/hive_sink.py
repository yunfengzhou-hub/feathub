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


# TODO: Support SQL grammars like `INSERT OVERWRITE` and `PARTITION` in
#  Feathub connectors.
class HiveSink(Sink):
    """
    A sink that write data to Hive.
    """

    def __init__(
        self,
        database: str,
        hive_conf_dir: Optional[str] = None,
        extra_config: Optional[Dict[str, str]] = None,
    ):
        """
        :param database: The database to write to.
        :param hive_conf_dir: URI to your Hive conf dir containing hive-site.xml.
                              The URI needs to be supported by Hadoop FileSystem.
                              If the URI is relative, i.e. without a scheme, local
                              file system is assumed. If the option is not specified,
                              hive-site.xml is searched in class path.
        :param extra_config: Extra configurations to be passthrough to the processor.
                             The available configurations are different for different
                             processors.
        """
        super().__init__(
            name="",
            system_name="hive",
            table_uri={
                "hive_conf_dir": hive_conf_dir,
                "database": database,
            },
        )
        self.hive_conf_dir = hive_conf_dir
        self.database = database
        self.extra_config = extra_config

    def to_json(self) -> Dict:
        return {
            "type": "HiveSink",
            "name": self.name,
            "hive_conf_dir": self.hive_conf_dir,
            "database": self.database,
            "extra_config": self.extra_config,
        }
