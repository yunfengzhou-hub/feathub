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
from abc import abstractmethod
from datetime import datetime

import pandas as pd
from testcontainers.redis import RedisContainer

from feathub.common import types
from feathub.common.types import Int64
from feathub.common.utils import (
    serialize_object_with_protobuf,
    to_unix_timestamp,
)
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.processors.processor import Processor
from feathub.processors.tests.processor_test_utils import ProcessorTestBase
from feathub.registries.registry import Registry
from feathub.table.schema import Schema


class RedisSourceSinkTestBase(ProcessorTestBase):
    """
    Base class that provides test cases to verify RedisSource and RedisSink.
    """

    __test__ = False

    redis_container: RedisContainer = None

    @abstractmethod
    def get_processor(self, registry: Registry) -> Processor:
        pass

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.redis_container = RedisContainer()
        cls.redis_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.redis_container.stop()

    def setUp(self) -> None:
        super().setUp()
        self.host = self.redis_container.get_container_host_ip()
        if self.host == "localhost":
            self.host = "127.0.0.1"
        self.port = self.redis_container.get_exposed_port(
            self.redis_container.port_to_expose
        )

        self.column_names = ["id", "val", "ts"]

        self.input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=self.column_names,
        )

        self.schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )

    def test_redis_sink(self):
        source = self._create_file_source(
            self.input_data.copy(),
            schema=self.schema,
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        sink = RedisSink(
            namespace="test_namespace",
            host=self.host,
            port=int(self.port),
        )

        self.processor.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait(30000)

        redis_client = self.redis_container.get_client()
        self.assertEquals(len(redis_client.keys("*")), self.input_data.shape[0])

        for i in range(self.input_data.shape[0]):
            key = b"test_namespace:" + serialize_object_with_protobuf(
                self.input_data["id"][i], Int64
            )

            self.assertEquals(
                {
                    int(0).to_bytes(4, byteorder="big"): serialize_object_with_protobuf(
                        i + 1, Int64
                    ),
                    b"__timestamp__": int(
                        to_unix_timestamp(
                            datetime(2022, 1, 1, 0, 0, i),
                            format="%Y-%m-%d %H:%M:%S",
                        )
                    ).to_bytes(8, byteorder="big"),
                },
                redis_client.hgetall(key.decode("utf-8")),
            )
