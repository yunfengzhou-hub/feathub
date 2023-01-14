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
from typing import Tuple

from feathub.feathub_client import FeathubClient

from feathub.common.types import Float64
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.processors.processor import Processor
from feathub.processors.tests.processor_test_utils import ProcessorTestBase, UnsupportedProcessorTests

unsupported_processor_test_class = [
    "SparkProcessorTest"
]
#
#
# def get_unsupported_test_class_name():
#     value = []
#     from feathub.processors.spark.tests.test_spark_processor_local_mode import SparkProcessorTest
#     value.append(SparkProcessorTest.__name__)
#     return value

def should_skip():
    stack = inspect.stack()
    # print("stack start")
    # for stack_element in stack:
    #     print(stack_element)
    # print("stack end")
    if "self" not in stack[1][0].f_locals:
        ret_value = False
    else:
        ret_value = (stack[1][0].f_locals["self"].__class__.__name__ in unsupported_processor_test_class)
    print("ret_value " + str(ret_value))
    return ret_value


def get_class_name():
    stack = inspect.stack()
    # print("get_class_namehaha")
    # print(stack[1][0].f_locals)
    # print(type(stack[1][0].f_locals))
    # print(stack[1][0].f_locals.__class__)
    if "self" in stack[1][0].f_locals:
        # print(stack[1][0].f_locals["self"].__class__.__name__)
        # print(stack[1][0].f_locals["self"].__class__.__name__ in unsupported_processor_test_class)
        return stack[1][0].f_locals["self"].__class__.__name__
    return None
    # the_class = stack[1][0].f_locals["self"].__class__.__name__
    # the_method = stack[1][0].f_code.co_name


# @unittest.skipIf(get_class_name() in unsupported_processor_test_class, "This test should be skipped")
@UnsupportedProcessorTests(["SparkProcessorTest"])
class ExpressionTransformTestBase(ProcessorTestBase):
    """
    Base class that provides test cases to verify ExpressionTransform.
    """

    __test__ = False

    @staticmethod
    def get_unsupported_test_cases() -> Tuple:
        return ("SparkProcessorTest",)

    # def __init__(self):
    #
    #     super().__init__(methodName=)

    @abstractmethod
    def get_client(self) -> FeathubClient:
        pass

    # def getTestCaseNames(self):

    # @unittest.skipIf(get_class_name() in unsupported_processor_test_class, "This test should be skipped")
    @unittest.skip("This test should be skipped")
    def test_expression_transform(self):
        get_class_name()
        self._test_expression_transform(False)

    @unittest.skipIf(should_skip(), "This test should be skipped")
    # @unittest.skip("This test should be skipped")
    def test_expression_transform_keep_source_fields(self):
        self._test_expression_transform(True)

    def _test_expression_transform(self, keep_source_fields: bool):
        print("_test_expression_transform")
        source = self._create_file_source(self.input_data.copy())

        f_cost_per_mile = Feature(
            name="cost_per_mile",
            dtype=Float64,
            transform="CAST(cost AS DOUBLE) / CAST(distance AS DOUBLE) + 10",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                f_cost_per_mile,
            ],
            keep_source_fields=keep_source_fields,
        )

        result_df = self.client.get_features(features).to_pandas()

        expected_result_df = self.input_data.copy()
        expected_result_df["cost_per_mile"] = expected_result_df.apply(
            lambda row: row["cost"] / row["distance"] + 10, axis=1
        )

        if keep_source_fields:
            result_df = result_df.sort_values(by=["name", "time"])
            expected_result_df = expected_result_df.sort_values(by=["name", "time"])
        else:
            result_df = result_df.sort_values(by=["time"])
            expected_result_df.drop(["name", "cost", "distance"], axis=1, inplace=True)
            expected_result_df = expected_result_df.sort_values(by=["time"])

        result_df = result_df.reset_index(drop=True)
        expected_result_df = expected_result_df.reset_index(drop=True)

        self.assertIsNone(source.keys)
        self.assertIsNone(features.keys)
        self.assertTrue(expected_result_df.equals(result_df))
