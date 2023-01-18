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
import os
import unittest

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
)

from feathub.feature_tables.sinks.kafka_sink import KafkaSink


class KafkaSourceSinkTest(unittest.TestCase):
    env = None
    t_env = None

    @classmethod
    def setUpClass(cls) -> None:
        # Due to the resource leak in PyFlink StreamExecutionEnvironment and
        # StreamTableEnvironment https://issues.apache.org/jira/browse/FLINK-30258.
        # We want to share env and t_env across all the tests in one class to mitigate
        # the leak.
        # TODO: After the ticket is resolved, we should clean up the resource in
        #  StreamExecutionEnvironment and StreamTableEnvironment after every test to
        #  fully avoid resource leak.
        cls.env = StreamExecutionEnvironment.get_execution_environment()
        cls.t_env = StreamTableEnvironment.create(cls.env)

    @classmethod
    def tearDownClass(cls) -> None:
        print("kafka tearDownClass is invoked")
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")

    def test_kafka_sink(self):
        KafkaSink(
            bootstrap_server="localhost:9092",
            topic="test-topic",
            key_format="json",
            value_format="json",
            producer_properties={"producer.key": "value"},
        )

        self.t_env.from_elements([(1,)])
