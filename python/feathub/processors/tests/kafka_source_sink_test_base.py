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
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.processors.tests.feathub_test_base import FeathubTestBase
from feathub.table.schema import Schema


class KafkaSourceSinkTestBase(FeathubTestBase):
    """
    Base class that provides test cases to verify KafkaSource and KafkaSink.
    """

    kafka_container = None

    @abstractmethod
    def get_client(self) -> FeathubClient:
        pass

    def test_kafka_source_sink(self):
        input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=["id", "val", "ts"],
        )

        schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )

        topic_name, start_time = self._produce_data_to_kafka(input_data, schema)

        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self._get_kafka_bootstrap_servers(),
            topic=topic_name,
            key_format="json",
            value_format="json",
            schema=schema,
            consumer_group="test-group",
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            startup_mode="timestamp",
            startup_datetime=start_time,
        )

        result_df = (
            self.client.get_features(source)
            .to_pandas(True)
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result_df = (
            input_data.copy().sort_values(by=["id"]).reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_bounded_kafka_source(self):
        input_data = pd.DataFrame(
            [
                [1, 1, datetime(2022, 1, 1, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S")],
                [2, 2, datetime(2022, 1, 1, 0, 0, 1).strftime("%Y-%m-%d %H:%M:%S")],
                [3, 3, datetime(2022, 1, 1, 0, 0, 2).strftime("%Y-%m-%d %H:%M:%S")],
            ],
            columns=["id", "val", "ts"],
        )

        schema = (
            Schema.new_builder()
            .column("id", types.Int64)
            .column("val", types.Int64)
            .column("ts", types.String)
            .build()
        )

        topic_name, start_time = self._produce_data_to_kafka(input_data, schema)

        # Consume data with kafka source
        source = KafkaSource(
            "kafka_source",
            bootstrap_server=self._get_kafka_bootstrap_servers(),
            topic=topic_name,
            key_format="json",
            value_format="json",
            schema=schema,
            consumer_group="test-group",
            keys=["id"],
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            startup_mode="timestamp",
            startup_datetime=start_time,
            is_bounded=True,
        )

        features = DerivedFeatureView(
            "feature_view", source, features=[], keep_source_fields=True
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        expected_result_df = (
            input_data.copy().sort_values(by=["id"]).reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    @classmethod
    def _get_kafka_bootstrap_servers(cls):
        if not hasattr(cls, "kafka_container") or cls.kafka_container is None:
            cls.kafka_container = KafkaContainer()
            cls.kafka_container.start()
        return cls.kafka_container.get_bootstrap_server()

    @classmethod
    def _tear_down_kafka(cls) -> None:
        if hasattr(cls, "kafka_container") and cls.kafka_container is not None:
            cls.kafka_container.stop()
            cls.kafka_container = None

    def _produce_data_to_kafka(self, input_data: pd.DataFrame, schema: Schema):
        source = self._create_file_source(
            input_data,
            keys=["id"],
            schema=schema,
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        topic_name = self._generate_random_name("kafka")

        sink = KafkaSink(
            bootstrap_server=self._get_kafka_bootstrap_servers(),
            topic=topic_name,
            key_format="json",
            value_format="json",
        )

        start_time = datetime.now()

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()

        return topic_name, start_time
