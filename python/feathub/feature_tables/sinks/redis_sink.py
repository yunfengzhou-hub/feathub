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
from feathub.feature_tables.sinks.sink import Sink
from feathub.feature_tables.sources.redis_source import RedisMode


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
    ):
        """
        :param mode: The deployment mode or the name of the mode of the redis service.
        :param host: The host of the Redis instance to connect.
        :param port: The port of the Redis instance to connect.
        :param username: The username used by the Redis authorization process.
        :param password: The password used by the Redis authorization process.
        :param db_num: The No. of the Redis database to connect. Not supported in
                       Cluster mode.
        :param namespace: The namespace to persist features in Redis. Feature tables
                          sinking to Redis sinks with different namespaces can save
                          records with the same key into Redis without overwriting
                          each other.
        """
        mode = mode if isinstance(mode, RedisMode) else RedisMode(mode)
        super().__init__(
            name="",
            system_name="redis",
            table_uri={
                "namespace": namespace,
                "host": host,
                "port": port,
                "mode": mode.name,
                "username": username,
                "password": password,
                "db_num": db_num,
            },
        )
        self.namespace = namespace
        self.host = host
        self.port = port
        self.mode = mode
        self.username = username
        self.password = password
        self.db_num = db_num

        if mode == RedisMode.CLUSTER and db_num != 0:
            raise FeathubException(
                "Selecting database is not supported in Cluster mode."
            )

    def to_json(self) -> Dict:
        return {
            "type": "RedisSink",
            "namespace": self.namespace,
            "host": self.host,
            "port": self.port,
            "mode": self.mode.name,
            "username": self.username,
            "password": self.password,
            "db_num": self.db_num,
        }
