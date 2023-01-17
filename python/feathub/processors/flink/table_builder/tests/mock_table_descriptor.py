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
import tempfile
import unittest
from abc import ABC
from typing import Optional, List, Dict

import pandas as pd
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from feathub.common.types import String, Int64
from feathub.feature_views.feature import Feature
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.flink.table_builder.flink_table_builder import FlinkTableBuilder
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    generate_random_table_name,
)
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class FlinkTableBuilderTestBase(unittest.TestCase):
    registry = None
    env = None
    t_env = None
    flink_table_builder = None

    @classmethod
    def setUpClass(cls) -> None:
        # Due to the resource leak in PyFlink StreamExecutionEnvironment and
        # StreamTableEnvironment https://issues.apache.org/jira/browse/FLINK-30258.
        # We want to share env and t_env across all the tests in one class to mitigate
        # the leak.
        # TODO: After the ticket is resolved, we should clean up the resource in
        #  StreamExecutionEnvironment and StreamTableEnvironment after every test to
        #  fully avoid resource leak.
        cls.registry = LocalRegistry(props={})
        cls.env = StreamExecutionEnvironment.get_execution_environment()
        cls.t_env = StreamTableEnvironment.create(cls.env)
        cls.flink_table_builder = FlinkTableBuilder(cls.t_env, registry=cls.registry)

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

        self.input_data = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:01:00"],
                ["Emma", 400, 250, "2022-01-01 08:02:00"],
                ["Alex", 300, 200, "2022-01-02 08:03:00"],
                ["Emma", 200, 250, "2022-01-02 08:04:00"],
                ["Jack", 500, 500, "2022-01-03 08:05:00"],
                ["Alex", 600, 800, "2022-01-03 08:06:00"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        self.schema = Schema(
            ["name", "cost", "distance", "time"], [String, Int64, Int64, String]
        )

    def tearDown(self) -> None:
        MemoryOnlineStore.get_instance().reset()

    @classmethod
    def tearDownClass(cls) -> None:
        if "PYFLINK_GATEWAY_DISABLED" in os.environ:
            os.environ.pop("PYFLINK_GATEWAY_DISABLED")


class MockTableDescriptor(TableDescriptor, ABC):
    def __init__(
        self,
        name: str = generate_random_table_name("mock_descriptor"),
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
    ) -> None:
        super().__init__(
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    def get_feature(self, feature_name: str) -> Feature:
        raise RuntimeError("Unsupported operation.")

    def is_bounded(self) -> bool:
        raise RuntimeError("Unsupported operation.")

    def get_bounded_view(self) -> TableDescriptor:
        raise RuntimeError("Unsupported operation.")

    def to_json(self) -> Dict:
        raise RuntimeError("Unsupported operation.")
