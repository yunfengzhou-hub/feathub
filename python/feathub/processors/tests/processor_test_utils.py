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
import shutil
import tempfile
import unittest
import uuid
from abc import abstractmethod
from typing import Optional, List, Dict, Set

import pandas as pd
import pytest

from feathub.common import types
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema


def get_pytest_params(excluded_tags: Set[str] = None) -> List:
    _processor_test_configs = {
        "flink": {
            "type": "flink",
            "flink": {
                "deployment_mode": "cli",
            },
        },
        "local": {
            "type": "local",
        },
        "spark": {
            "type": "spark",
            "spark": {
                "master": "local[1]",
            },
        },
    }

    pytest_params = []
    for tag, processor_config in _processor_test_configs:
        if excluded_tags is not None and tag in excluded_tags:
            continue

        client_config = (
            {
                "processor": processor_config,
                "online_store": {
                    "types": ["memory"],
                    "memory": {},
                },
                "registry": {
                    "type": "local",
                    "local": {
                        "namespace": "default",
                    },
                },
                "feature_service": {
                    "type": "local",
                    "local": {},
                },
            }
        )

        pytest_params.append(pytest.param(client_config, id=tag))

    return pytest_params


class ProcessorTestBase(unittest.TestCase):
    """
    Abstract base class for all processor integration tests. A child class of
    this class must have its test cases use Feathub public APIs to get and write
    features, and instantiate the corresponding Processor instance.

    This class also provides utility variables and methods to assist the construction
    of test cases.
    """

    __test__ = False

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()
        self.registry = LocalRegistry(props={})
        self.input_data, self.schema = self._create_input_data_and_schema()

    def tearDown(self) -> None:
        MemoryOnlineStore.get_instance().reset()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @abstractmethod
    def get_client(self, client_config: Dict) -> FeathubClient:
        """
        Returns a Processor instance for test cases.
        """
        return FeathubClient(
            props=client_config
        )

    def _create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        schema: Optional[Schema] = None,
        timestamp_field: Optional[str] = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
        name: str = None,
    ) -> FileSystemSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        if schema is None:
            schema = self._create_input_schema()
        df.to_csv(path, index=False, header=False)

        if name is None:
            name = self.__generate_random_name("source")

        return FileSystemSource(
            name=name,
            path=path,
            data_format="csv",
            schema=schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

    def _create_input_data_and_schema(self):
        input_data = pd.DataFrame(
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

        schema = self._create_input_schema()

        return input_data, schema

    def _create_input_data_and_schema_with_millis_time_span(self):
        input_data = pd.DataFrame(
            [
                ["Alex", 100, 100, "2022-01-01 08:00:00.001"],
                ["Emma", 400, 250, "2022-01-01 08:00:00.002"],
                ["Alex", 300, 200, "2022-01-01 08:00:00.003"],
                ["Emma", 200, 250, "2022-01-01 08:00:00.004"],
                ["Jack", 500, 500, "2022-01-01 08:00:00.005"],
                ["Alex", 600, 800, "2022-01-01 08:00:00.006"],
            ],
            columns=["name", "cost", "distance", "time"],
        )

        schema = self._create_input_schema()

        return input_data, schema

    @staticmethod
    def _create_input_schema():
        return (
            Schema.new_builder()
            .column("name", types.String)
            .column("cost", types.Int64)
            .column("distance", types.Int64)
            .column("time", types.String)
            .build()
        )

    @staticmethod
    def __generate_random_name(root_name: str) -> str:
        random_name = f"{root_name}_{str(uuid.uuid4()).replace('-', '')}"
        return random_name
