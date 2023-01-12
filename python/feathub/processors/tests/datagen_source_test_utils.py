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

from feathub.common.types import Int32, Timestamp
from feathub.feature_tables.sources.datagen_source import (
    DataGenSource,
    SequenceField,
    RandomField,
)
from feathub.processors.processor import Processor
from feathub.processors.tests.processor_test_utils import ProcessorTestBase
from feathub.registries.registry import Registry
from feathub.table.schema import Schema


class DataGenSourceTestBase(ProcessorTestBase):
    """
    Base class that provides test cases to verify DataGenSource.
    """

    __test__ = False

    @abstractmethod
    def get_processor(self, registry: Registry) -> Processor:
        pass

    def test_data_gen_source(self):
        source = DataGenSource(
            name="datagen_src",
            rows_per_second=10,
            field_configs={
                "id": SequenceField(start=0, end=9),
                "val": RandomField(minimum=0, maximum=100),
            },
            schema=Schema(["id", "val", "ts"], [Int32, Int32, Timestamp]),
            timestamp_field="ts",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

        df = self.processor.get_table(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["val"] >= 0).all() and (df["val"] <= 100).all())
