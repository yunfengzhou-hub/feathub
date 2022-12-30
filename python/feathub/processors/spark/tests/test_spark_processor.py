# Copyright 2022 The Feathub Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import glob
import shutil
import tempfile
import unittest
from typing import Optional, List

import pandas as pd

from feathub.common import types
from feathub.common.types import from_numpy_dtype
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.online_stores.memory_online_store import MemoryOnlineStore
from feathub.processors.spark.spark_processor import SparkProcessor
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema


class SparkProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = LocalRegistry(props={})
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        MemoryOnlineStore.get_instance().reset()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_materialize_features(self) -> None:
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

        schema = (
            Schema.new_builder()
            .column("name", types.String)
            .column("cost", types.Int32)
            .column("distance", types.Int32)
            .column("time", types.String)
            .build()
        )

        processor = SparkProcessor(
            props={"processor.spark.master": "local[1]"},
            registry=self.registry,
        )

        source = self._create_file_source(input_data, schema=schema)

        sink_path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name

        sink = FileSystemSink(sink_path, "csv")

        processor.materialize_features(
            features=source,
            sink=sink,
        ).wait()

        files = glob.glob(f"{sink_path}/*.csv")
        df = pd.DataFrame()
        for f in files:
            csv = pd.read_csv(f, names=["name", "cost", "distance", "time"])
            df = df.append(csv)
        df = df.sort_values(by=["time"]).reset_index(drop=True)
        self.assertTrue(input_data.equals(df))

    def _create_file_source(
        self,
        df: pd.DataFrame,
        keys: Optional[List[str]] = None,
        schema: Schema = None,
        timestamp_field: str = "time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> FileSystemSource:
        path = tempfile.NamedTemporaryFile(dir=self.temp_dir).name
        if schema is None:
            schema = Schema(
                field_names=df.keys().tolist(),
                field_types=[from_numpy_dtype(dtype) for dtype in df.dtypes],
            )
        df.to_csv(path, index=False, header=False)

        return FileSystemSource(
            name="source",
            path=path,
            data_format="csv",
            schema=schema,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
