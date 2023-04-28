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
from typing import Optional, Dict

from feathub.table.schema import Schema


class HiveConfig:
    def __init__(
            self,
            name: str,
            hive_conf_dir: Optional[str],
            default_database: Optional[str],
            hadoop_conf_dir: Optional[str],
    ):
        self.name = name
        self.hive_conf_dir = hive_conf_dir
        self.default_database = default_database
        self.hadoop_conf_dir = hadoop_conf_dir

    def to_json(self) -> Dict:
        pass


class HiveSource:
    def __init__(
        self,
        name: str,
        schema: Schema,
        hive_config: HiveConfig,
    ):
        self.name = name
        self.schema = schema
        self.hive_config = hive_config

