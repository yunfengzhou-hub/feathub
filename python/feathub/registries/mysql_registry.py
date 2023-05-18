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
import json
from hashlib import sha256
from typing import List, Optional, Dict, Any, Tuple

import mysql.connector

from feathub.common.config import ConfigDef
from feathub.common.exceptions import FeathubException
from feathub.registries.registry import Registry
from feathub.registries.registry_config import REGISTRY_PREFIX, RegistryConfig
from feathub.table.table_descriptor import TableDescriptor

MYSQL_REGISTRY_PREFIX = REGISTRY_PREFIX + "mysql."

DATABASE_CONFIG = MYSQL_REGISTRY_PREFIX + "database"
DATABASE_DOC = "The name of the MySQL database to hold the Feathub registry."

TABLE_CONFIG = MYSQL_REGISTRY_PREFIX + "table"
TABLE_DOC = "The name of the MySQL table to hold the Feathub registry."

HOST_CONFIG = MYSQL_REGISTRY_PREFIX + "host"
HOST_DOC = "IP address or hostname of the MySQL server."

PORT_CONFIG = MYSQL_REGISTRY_PREFIX + "port"
PORT_DOC = "The port of the MySQL server."

USERNAME_CONFIG = MYSQL_REGISTRY_PREFIX + "username"
USERNAME_DOC = "Name of the user to connect to the MySQL server."

PASSWORD_CONFIG = MYSQL_REGISTRY_PREFIX + "password"
PASSWORD_DOC = "The password of the user."


mysql_registry_config_defs: List[ConfigDef] = [
    ConfigDef(
        name=DATABASE_CONFIG,
        value_type=str,
        description=DATABASE_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=TABLE_CONFIG,
        value_type=str,
        description=TABLE_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=HOST_CONFIG,
        value_type=str,
        description=HOST_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=PORT_CONFIG,
        value_type=int,
        description=PORT_DOC,
        default_value=3306,
    ),
    ConfigDef(
        name=USERNAME_CONFIG,
        value_type=str,
        description=USERNAME_DOC,
        default_value=None,
    ),
    ConfigDef(
        name=PASSWORD_CONFIG,
        value_type=str,
        description=PASSWORD_DOC,
        default_value=None,
    ),
]


class MySqlRegistryConfig(RegistryConfig):
    def __init__(self, props: Dict[str, Any]) -> None:
        super().__init__(props)
        self.update_config_values(mysql_registry_config_defs)


def _get_digest(json_dict: Dict) -> str:
    return sha256(json.dumps(json_dict, sort_keys=True).encode("utf8")).hexdigest()


class MySqlRegistry(Registry):
    """
    A registry that stores entities in a MySQL database.
    """

    def __init__(
        self,
        props: Dict,
    ):
        super().__init__("mysql", props)
        mysql_registry_config = MySqlRegistryConfig(props)
        self.database = mysql_registry_config.get(DATABASE_CONFIG)
        self.table = mysql_registry_config.get(TABLE_CONFIG)
        self.host = mysql_registry_config.get(HOST_CONFIG)
        self.port = mysql_registry_config.get(PORT_CONFIG)
        self.username = mysql_registry_config.get(USERNAME_CONFIG)
        self.password = mysql_registry_config.get(PASSWORD_CONFIG)
        self.conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database,
        )
        self.cursor = self.conn.cursor()

        self.cursor.execute(
            f"""
                CREATE TABLE IF NOT EXISTS `{self.table}`(
                   `digest` VARCHAR(64) NOT NULL,
                   `name` TEXT NOT NULL,
                   `timestamp` TIMESTAMP NOT NULL,
                   `is_deleted` BOOLEAN NOT NULL,
                   `json_representation` TEXT,
                   PRIMARY KEY ( `digest`, `timestamp` )
                );
            """
        )

    def build_features(
        self, features_list: List[TableDescriptor], props: Optional[Dict] = None
    ) -> List[TableDescriptor]:
        result = []
        for table in features_list:
            if table.name == "":
                raise FeathubException(
                    "Cannot build a TableDescriptor with empty name."
                )
            built_table = table.build(self, props)
            self.register_features(built_table)
            result.append(built_table)

        return result

    def register_features(
        self, features: TableDescriptor, override: bool = True
    ) -> bool:
        try:
            existing_features = self.get_features(features.name)

            if existing_features.to_json() == features.to_json():
                return True
            elif not override:
                return False
        except RuntimeError as e:
            if str(e) != (
                f"Table '{features.name}' is not found in the cache or registry. "
                f"Please invoke build_features(..) for this table."
            ):
                raise e

        json_dict = features.to_json()
        json_dict_str = json.dumps(json_dict, sort_keys=True).replace('"', '\\"')
        self.cursor.execute(
            f"""
                INSERT INTO {self.table} (
                   `digest`,
                   `name`,
                   `timestamp`,
                   `is_deleted`,
                   `json_representation`
                ) VALUES (
                    "{_get_digest(json_dict)}",
                    "{features.name}",
                    NOW(),
                    False,
                    "{json_dict_str}"
                );
            """
        )
        return True

    def get_features(self, name: str) -> TableDescriptor:
        self.cursor.execute(
            f"""
                SELECT
                    `digest`,
                    `json_representation`,
                    `is_deleted`
                FROM {self.table}
                WHERE `name` = "{name}"
                ORDER BY `timestamp` DESC
                LIMIT 1;
            """
        )
        results: List[Tuple] = self.cursor.fetchall()
        feature_exists = results[0][2] == 0 if len(results) > 0 else False
        if not feature_exists:
            raise RuntimeError(
                f"Table '{name}' is not found in the cache or registry. "
                "Please invoke build_features(..) for this table."
            )
        digest, json_representation, _ = results[0]
        json_dict = json.loads(json_representation)
        if _get_digest(json_dict) != digest:
            raise FeathubException(
                f"Acquired features's json representation cannot match the digest. "
                f"Data might be broken. Json representation: {json_representation}, "
                f"digest: {digest}"
            )
        return TableDescriptor.from_json(json_dict)

    def delete_features(self, name: str) -> bool:
        self.cursor.execute(
            f"""
                UPDATE {self.table}
                SET `is_deleted`=True
                WHERE `name` = "{name}"
                ORDER BY `timestamp` DESC
                LIMIT 1;
            """
        )
        return True

    def __del__(self) -> None:
        self.cursor.close()
        self.conn.close()
