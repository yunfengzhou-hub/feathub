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
import inspect
import unittest
from abc import abstractmethod

from feathub.common.types import Int32, Timestamp
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sources.datagen_source import (
    DataGenSource,
    SequenceField,
    RandomField,
)
from feathub.processors.tests.processor_test_utils import ProcessorTestBase
from feathub.table.schema import Schema


def get_class_name():
    stack = inspect.stack()
    print(stack[1][0])
    # the_class = stack[1][0].f_locals["self"].__class__.__name__
    # the_method = stack[1][0].f_code.co_name


# @unittest.skipIf(condition=__class__.name in names, reason="")
class DataGenSourceTestBase(ProcessorTestBase):
    """
    Base class that provides test cases to verify DataGenSource.
    """

    names = []

    __test__ = False

    @abstractmethod
    def get_client(self) -> FeathubClient:
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

        df = self.client.get_features(features=source).to_pandas()

        self.assertEquals(10, df.shape[0])
        self.assertTrue((df["val"] >= 0).all() and (df["val"] <= 100).all())
