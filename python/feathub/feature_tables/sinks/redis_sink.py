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
from typing import Dict, Union

from feathub.common.exceptions import FeathubException
from feathub.common.utils import append_metadata_to_json
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_tables.sources.redis_source import (
    RedisMode,
    NAMESPACE_KEYWORD,
    FEATURE_NAME_KEYWORD,
)


class RedisSink(Sink):
    def __init__(
        self,
        host: str,
        port: int = 6379,
        mode: Union[RedisMode, str] = RedisMode.STANDALONE,
        username: str = None,
        password: str = None,
        db_num: int = 0,
        namespace: str = "default",
        key_expr: str = 'CONCAT_WS(":", __NAMESPACE__, __KEYS__, __FEATURE_NAME__)',
    ):
        """
        :param host: The host of the Redis instance to connect.
        :param port: The port of the Redis instance to connect.
        :param mode: The deployment mode or the name of the mode of the redis service.
        :param username: The username used by the Redis authorization process.
        :param password: The password used by the Redis authorization process.
        :param db_num: The No. of the Redis database to connect. Not supported in
                       Cluster mode.
        :param namespace: The namespace to persist features in Redis. Feature tables
                          sinking to Redis sinks with different namespaces can save
                          records with the same key into Redis without overwriting
                          each other.
        :param key_expr: A string that represents a FeatHub expression which evaluates
                         to a string value, which would be used as the key to a feature
                         saved in Redis. Apart from the field names, UDFs and other
                         grammars supported by Feathub expression, users may also use
                         the following keywords in this expression, which are
                         dynamically evaluated during compilation according to other
                         configurations or the structure of feature tables.
                         - __NAMESPACE__: the namespace to persist features in Redis.
                         - __KEYS__: A colon separated list of all key field names.
                         - __FEATURE_NAME__: the name of a feature to be written out
                           to Redis.
                         If not explicitly specified, the key would be a combination of
                         the namespace, all key field values, and the name of the
                         feature.
        """
        super().__init__(
            name="",
            system_name="redis",
            table_uri={
                "host": host,
                "port": port,
                "db_num": db_num,
                "namespace": namespace,
            },
        )
        self.namespace = namespace
        self.host = host
        self.port = port
        self.mode = mode if isinstance(mode, RedisMode) else RedisMode(mode)
        self.username = username
        self.password = password
        self.db_num = db_num
        self.key_expr = key_expr

        if NAMESPACE_KEYWORD not in key_expr or FEATURE_NAME_KEYWORD not in key_expr:
            raise FeathubException(
                f"key_expr {key_expr} should contain {NAMESPACE_KEYWORD} and "
                f"{FEATURE_NAME_KEYWORD} in order to guarantee the uniqueness of "
                f"feature keys in Redis."
            )

        if mode == RedisMode.CLUSTER and db_num != 0:
            raise FeathubException(
                "Selecting database is not supported in Cluster mode."
            )

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "namespace": self.namespace,
            "host": self.host,
            "port": self.port,
            "mode": self.mode.name,
            "username": self.username,
            "password": self.password,
            "db_num": self.db_num,
            "key_expr": self.key_expr,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "RedisSink":
        return RedisSink(
            namespace=json_dict["namespace"],
            host=json_dict["host"],
            port=json_dict["port"],
            mode=json_dict["namespace"],
            username=json_dict["username"],
            password=json_dict["password"],
            db_num=json_dict["db_num"],
            key_expr=json_dict["key_expr"],
        )
