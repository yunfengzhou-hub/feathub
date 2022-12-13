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
import unittest
from datetime import datetime
from unittest.mock import patch

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    DataTypes,
    TableDescriptor as NativeFlinkTableDescriptor,
)


from testcontainers.kafka import KafkaContainer
from testcontainers.redis import RedisContainer

from feathub.common.types import Int64, String
from feathub.common.utils import serialize_object_with_protobuf, to_unix_timestamp
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.feature_tables.sources.online_store_source import OnlineStoreSource
from feathub.processors.flink.table_builder.flink_table_builder import FlinkTableBuilder
from feathub.processors.flink.table_builder.source_sink_utils import (
    insert_into_sink,
)
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class SinkUtilTest(unittest.TestCase):
    def test_redis_sink(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)
        sink = RedisSink(
            namespace="test_namespace",
            host="127.0.0.1",
            port=6379,
            password="123456",
            db_num=3,
        )

        table = t_env.from_elements([(1,)]).alias("id")
        with patch.object(
            t_env, "create_temporary_table"
        ) as create_temporary_table, patch("pyflink.table.table.Table.execute_insert"):
            placeholder_descriptor: TableDescriptor = OnlineStoreSource(
                "table_name_1", ["id"], "memory", "table_name_1"
            )

            insert_into_sink(t_env, table, placeholder_descriptor, sink)
            flink_table_descriptor: NativeFlinkTableDescriptor = (
                create_temporary_table.call_args[0][1]
            )

            expected_options = {
                "connector": "redis",
                "namespace": "test_namespace",
                "host": "127.0.0.1",
                "port": "6379",
                "password": "123456",
                "dbNum": "3",
                "keyField": "__redis_sink_key__",
            }
            self.assertEquals(
                expected_options, dict(flink_table_descriptor.get_options())
            )


class SourceSinkITTest(unittest.TestCase):
    kafka_container: KafkaContainer = None
    redis_container: RedisContainer = None

    def __init__(self, method_name: str):
        super().__init__(method_name)
        self.kafka_bootstrap_servers = None
        self.kafka_topic_name = method_name

    @classmethod
    def setUpClass(cls) -> None:
        cls.redis_container = RedisContainer()
        cls.redis_container.start()
        cls.kafka_container = KafkaContainer()
        cls.kafka_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.redis_container.stop()
        cls.kafka_container.stop()

    def setUp(self) -> None:
        self.host = SourceSinkITTest.redis_container.get_container_host_ip()
        self.port = SourceSinkITTest.redis_container.get_exposed_port(
            SourceSinkITTest.redis_container.port_to_expose
        )

        self.kafka_bootstrap_servers = (
            SourceSinkITTest.kafka_container.get_bootstrap_server()
        )

        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.t_env = StreamTableEnvironment.create(self.env)
        self.test_time = datetime.now()

        self.row_data = self._produce_data_to_kafka(self.t_env)

    def test_redis_sink(self):
        table_builder = FlinkTableBuilder(self.t_env, LocalRegistry({}))
        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.kafka_topic_name,
            key_format="json",
            value_format="json",
            schema=Schema(["id", "val", "ts"], [Int64, Int64, String]),
            consumer_group="test-group",
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            startup_mode="timestamp",
            startup_datetime=self.test_time,
            is_bounded=True,
        )

        table = table_builder.build(source)

        sink = RedisSink(
            namespace="test_namespace",
            host=self.host,
            port=int(self.port),
        )

        insert_into_sink(self.t_env, table, source, sink).wait(30000)

        redis_client = self.redis_container.get_client()
        self.assertEquals(len(redis_client.keys("*")), len(self.row_data))

        for i in range(len(self.row_data)):
            key = b"test_namespace:" + serialize_object_with_protobuf(
                self.row_data[i][0], Int64
            )

            if i != 3:
                self.assertEquals(
                    {
                        int(0).to_bytes(
                            4, byteorder="big"
                        ): serialize_object_with_protobuf(i + 1, Int64),
                        b"__timestamp__": int(
                            to_unix_timestamp(
                                datetime(2022, 1, 1, 0, 0, i),
                                format="%Y-%m-%d %H:%M:%S",
                            )
                        ).to_bytes(8, byteorder="big"),
                    },
                    redis_client.hgetall(key.decode("utf-8")),
                )
            else:
                self.assertEquals(
                    {
                        b"__timestamp__": int(
                            to_unix_timestamp(
                                datetime(2022, 1, 1, 0, 0, i),
                                format="%Y-%m-%d %H:%M:%S",
                            )
                        ).to_bytes(8, byteorder="big"),
                    },
                    redis_client.hgetall(key.decode("utf-8")),
                )

    def _produce_data_to_kafka(self, t_env):
        row_data = [
            (1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")),
            (2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")),
            (3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")),
            (4, None, datetime(2022, 1, 1, 0, 0, 3).strftime("%Y-%m-%d %H:%M:%S")),
        ]
        table = t_env.from_elements(
            row_data,
            DataTypes.ROW(
                [
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("val", DataTypes.BIGINT()),
                    DataTypes.FIELD("ts", DataTypes.STRING()),
                ]
            ),
        )
        sink = KafkaSink(
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.kafka_topic_name,
            key_format="json",
            value_format="json",
        )

        placeholder_descriptor: TableDescriptor = OnlineStoreSource(
            "placeholder", ["id"], "memory", "table_name_1"
        )

        insert_into_sink(t_env, table, placeholder_descriptor, sink).wait()
        return row_data
