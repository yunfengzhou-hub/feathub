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
from unittest.mock import patch

from pyflink.table import (
    TableDescriptor as NativeFlinkTableDescriptor,
)

from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.processors.flink.table_builder.source_sink_utils import (
    insert_into_sink,
)
from feathub.processors.flink.table_builder.tests.mock_table_descriptor import (
    MockTableDescriptor,
    FlinkTableBuilderTestBase,
)
from feathub.table.table_descriptor import TableDescriptor


class RedisSourceSinkTest(FlinkTableBuilderTestBase):
    def test_redis_sink(self):
        sink = RedisSink(
            namespace="test_namespace",
            host="127.0.0.1",
            port=6379,
            password="123456",
            db_num=3,
        )

        table = self.t_env.from_elements([(1,)]).alias("id")
        with patch.object(
            self.t_env, "create_temporary_table"
        ) as create_temporary_table, patch("pyflink.table.table.Table.execute_insert"):
            descriptor: TableDescriptor = MockTableDescriptor(keys=["id"])

            insert_into_sink(self.t_env, table, descriptor, sink)
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
