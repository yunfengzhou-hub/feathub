# Copyright 2022 The FeatHub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import List, Optional, Dict

from feathub.registries.registry import Registry
from feathub.table.table_descriptor import TableDescriptor


class MySqlRegistry(Registry):
    """
    A registry that stores entities in a MySQL database.
    """

    def __init__(
        self,
        database: str,
        table: str,
        host: str,
        username: str,
        password: str,
        port: int,
        config: Dict,
    ):
        super().__init__("mysql", config)
        pass

    def build_features(
        self, features_list: List[TableDescriptor], props: Optional[Dict] = None
    ) -> List[TableDescriptor]:
        pass

    def register_features(
        self, features: TableDescriptor, override: bool = True
    ) -> bool:
        pass

    def get_features(self, name: str) -> TableDescriptor:
        pass

    def delete_features(self, name: str) -> bool:
        pass
