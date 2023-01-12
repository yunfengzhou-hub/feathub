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
from testcontainers.kafka import KafkaContainer

from feathub.common import types
from feathub.common.types import Int64, String
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.processors.processor import Processor
from feathub.processors.tests.processor_test_utils import ProcessorTestBase
from feathub.registries.registry import Registry
from feathub.table.schema import Schema


class KafkaSourceSinkTestBase(ProcessorTestBase):
    """
    Base class that provides test cases to verify KafkaSource and KafkaSink.
    """

    __test__ = False

    kafka_container: KafkaContainer = None

    @abstractmethod
    def get_processor(self, registry: Registry) -> Processor:
        pass

    def __init__(self, methodName: str):
        super().__init__(methodName)
        self.kafka_bootstrap_servers = None
        self.topic_name = methodName

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.kafka_container = KafkaContainer()
        cls.kafka_container.start()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        cls.kafka_container.stop()

    def setUp(self) -> None:
        super().setUp()
        self.kafka_bootstrap_servers = self.kafka_container.get_bootstrap_server()

        self.test_time = datetime.now()

        self.input_data, self.schema = self._create_input_data_and_schema()

        self._produce_data_to_kafka()

    def test_kafka_source_sink(self):
        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.topic_name,
            key_format="json",
            value_format="json",
            schema=Schema(["id", "val", "ts"], [Int64, Int64, String]),
            consumer_group="test-group",
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            startup_mode="timestamp",
            startup_datetime=self.test_time,
        )

        result_df = (
            self.processor.get_table(source)
            .to_pandas(True)
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result_df = (
            self.input_data.copy().sort_values(by=["id"]).reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_bounded_kafka_source(self):
        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.topic_name,
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

        features = DerivedFeatureView(
            "feature_view", source, features=[], keep_source_fields=True
        )

        result_df = (
            self.processor.get_table(features)
            .to_pandas()
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result_df = (
            self.input_data.copy().sort_values(by=["id"]).reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def _produce_data_to_kafka(self):
        source = self._create_file_source(
            self.input_data,
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        sink = KafkaSink(
            bootstrap_server=self.kafka_bootstrap_servers,
            topic=self.topic_name,
            key_format="json",
            value_format="json",
        )

        self.processor.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

    def _create_input_data_and_schema(self):
        input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=["id", "val", "ts"],
        )

        schema = self._create_input_schema()

        return input_data, schema

    @staticmethod
    def _create_input_schema():
        return (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )
