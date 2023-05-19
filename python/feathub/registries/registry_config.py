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

from enum import Enum
from typing import Dict, Any

from feathub.common.config import ConfigDef, BaseConfig
from feathub.common.validators import in_list


class RegistryType(Enum):
    LOCAL = "local"
    MYSQL = "mysql"


REGISTRY_PREFIX = "registry."

REGISTRY_TYPE_CONFIG = REGISTRY_PREFIX + "type"
REGISTRY_TYPE_DOC = "The type of the registry to use."

registry_config_defs = [
    ConfigDef(
        name=REGISTRY_TYPE_CONFIG,
        value_type=str,
        description=REGISTRY_TYPE_DOC,
        default_value="local",
        validator=in_list(*[t.value for t in RegistryType]),
    )
]


class RegistryConfig(BaseConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(registry_config_defs)
