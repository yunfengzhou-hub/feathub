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
from abc import ABC
from datetime import datetime, timezone

import pandas as pd
import pytz

from feathub.common.types import Float64, Int64, String
from feathub.common.utils import to_unix_timestamp
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class ExpressionTransformITTest(ABC, FeathubITTestBase):
    def test_expression_transform(self):
        self._test_expression_transform(False)

    def test_expression_transform_keep_source_fields(self):
        self._test_expression_transform(True)

    def _test_expression_transform(self, keep_source_fields: bool):
        source = self.create_file_source(self.input_data.copy())

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

    def test_unix_timestamp(self):
        source = self.create_file_source(self.input_data.copy())

        unix_time = Feature(
            name="unix_time",
            dtype=Int64,
            transform="UNIX_TIMESTAMP(time)",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                unix_time,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = self.input_data.copy()
        expected_result_df["unix_time"] = expected_result_df.apply(
            lambda row: int(
                to_unix_timestamp(row["time"])
            ),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        print(expected_result_df)
        print(result_df)

        self.assertTrue(expected_result_df.equals(result_df))

    def test_unix_timestamp_with_timezone(self):
        input_data = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 08:00:00.001 +0800"],
                ["Emma", 400.0, "2022-01-01 00:00:00.003 +0000"],
                ["Alex", 200.0, "2022-01-01 08:00:00.005 +0800"],
                ["Emma", 300.0, "2022-01-01 00:00:00.007 +0000"],
                ["Jack", 500.0, "2022-01-01 08:00:00.009 +0800"],
                ["Alex", 450.0, "2022-01-01 00:00:00.011 +0000"],
            ],
            columns=["name", "avg_cost", "time"],
        )

        source = self.create_file_source(
            input_data.copy(),
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
        )

        unix_time = Feature(
            name="unix_time",
            dtype=Int64,
            transform="UNIX_TIMESTAMP(time, '%Y-%m-%d %H:%M:%S.%f %z')",
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                unix_time,
            ],
            keep_source_fields=True,
        )

        result_df = (
            self.client.get_features(features)
            .to_pandas()
            .sort_values(by=["time"])
            .reset_index(drop=True)
        )

        expected_result_df = input_data.copy()
        expected_result_df["unix_time"] = expected_result_df.apply(
            lambda row: int(
                to_unix_timestamp(row["time"], format="%Y-%m-%d %H:%M:%S.%f %z")
            ),
            axis=1,
        )
        expected_result_df = expected_result_df.sort_values(by=["time"]).reset_index(
            drop=True
        )

        self.assertTrue(expected_result_df.equals(result_df))
