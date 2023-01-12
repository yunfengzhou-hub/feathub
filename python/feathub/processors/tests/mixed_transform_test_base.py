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
from datetime import timedelta
from math import sqrt

import pandas as pd

from feathub.common.test_utils import to_epoch_millis
from feathub.common.types import Float64, String, Int64
from feathub.feathub_client import FeathubClient
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import SlidingFeatureView
from feathub.feature_views.transforms.over_window_transform import OverWindowTransform
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.tests.feathub_test_base import FeathubTestBase
from feathub.processors.tests.sliding_window_transform_test_base import (
    ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT,
    DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
    ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
)
from feathub.table.schema import Schema


class MixedTransformTestBase(FeathubTestBase):
    """
    Base class that verifies two or more transformations in one test case.
    """

    @abstractmethod
    def get_client(self) -> FeathubClient:
        pass

    def test_python_udf_transform_on_over_window_transform(self):
        df, schema = self._create_input_data_and_schema_with_millis_time_span()

        source = self._create_file_source(
            df, schema=schema, timestamp_format="%Y-%m-%d %H:%M:%S.%f"
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="lower_name",
                    dtype=String,
                    transform=PythonUdfTransform(lambda row: row["name"].lower()),
                ),
                Feature(
                    name="cost_sum",
                    dtype=Int64,
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["lower_name"],
                        window_size=timedelta(milliseconds=3),
                    ),
                ),
                Feature(
                    name="cost_sum_sqrt",
                    dtype=Float64,
                    transform=PythonUdfTransform(lambda row: sqrt(row["cost_sum"])),
                ),
            ],
        )

        expected_result_df = df
        expected_result_df["lower_name"] = expected_result_df["name"].apply(
            lambda x: x.lower()
        )
        expected_result_df["cost_sum"] = pd.Series([100, 400, 400, 600, 500, 900])
        expected_result_df["cost_sum_sqrt"] = expected_result_df["cost_sum"].apply(
            lambda x: sqrt(x)
        )
        expected_result_df.drop(["name", "cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["lower_name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["lower_name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_join_sliding_feature(self):
        df = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01 09:01:00"],
                ["Alex", 200.0, "2022-01-01 09:01:20"],
                ["Alex", 450.0, "2022-01-01 09:06:00"],
            ],
            columns=["name", "cost", "time"],
        )

        schema = Schema(["name", "cost", "time"], [String, Float64, String])
        source = self._create_file_source(
            df, schema=schema, keys=["name"], name="source"
        )

        df2 = pd.DataFrame(
            [
                ["Alex", "2022-01-01 09:01:00"],
                ["Alex", "2022-01-01 09:02:00"],
                ["Alex", "2022-01-01 09:05:00"],
                ["Alex", "2022-01-01 09:07:00"],
                ["Alex", "2022-01-01 09:09:00"],
            ]
        )
        source2 = self._create_file_source(
            df2,
            schema=Schema(["name", "time"], [String, String]),
            keys=["name"],
            name="source2",
        )

        expected_results = [
            (
                ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT,
                pd.DataFrame(
                    [
                        ["Alex", "2022-01-01 09:01:00", None, None],
                        ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                        ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                        ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                        ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                    ],
                    columns=["name", "time", "last_2_minute_total_cost", "cnt"],
                ),
            ),
            (
                DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
                pd.DataFrame(
                    [
                        ["Alex", "2022-01-01 09:01:00", 0.0, 0],
                        ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                        ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                        ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                        ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                    ],
                    columns=["name", "time", "last_2_minute_total_cost", "cnt"],
                ),
            ),
            (
                ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
                pd.DataFrame(
                    [
                        ["Alex", "2022-01-01 09:01:00", None, None],
                        ["Alex", "2022-01-01 09:02:00", 300.0, 2],
                        ["Alex", "2022-01-01 09:05:00", 0.0, 0],
                        ["Alex", "2022-01-01 09:07:00", 450.0, 1],
                        ["Alex", "2022-01-01 09:09:00", 0.0, 0],
                    ],
                    columns=["name", "time", "last_2_minute_total_cost", "cnt"],
                ),
            ),
        ]

        for props, expected_result_df in expected_results:
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[
                    Feature(
                        name="last_2_minute_total_cost",
                        dtype=Float64,
                        transform=SlidingWindowTransform(
                            expr="cost",
                            agg_func="SUM",
                            group_by_keys=["name"],
                            window_size=timedelta(minutes=2),
                            step_size=timedelta(minutes=1),
                        ),
                    ),
                    Feature(
                        name="cnt",
                        dtype=Int64,
                        transform=SlidingWindowTransform(
                            expr="1",
                            agg_func="COUNT",
                            group_by_keys=["name"],
                            window_size=timedelta(minutes=2),
                            step_size=timedelta(minutes=1),
                        ),
                    ),
                ],
                props=props,
            )

            joined_feature = DerivedFeatureView(
                name="joined_feature",
                source=source2,
                features=["features.last_2_minute_total_cost", "features.cnt"],
                keep_source_fields=True,
            )
            self.client.build_features([features])

            built_joined_feature = self.client.build_features([joined_feature])[0]

            expected_result_df = expected_result_df.sort_values(
                by=["name", "time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(features=built_joined_feature)
                .to_pandas()
                .sort_values(by=["name", "time"])
                .reset_index(drop=True)
            )

            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_sliding_window_with_python_udf(self):
        df = self.input_data.copy()
        source = self._create_file_source(df)

        def name_to_lower(row: pd.Series) -> str:
            return row["name"].lower()

        f_lower_name = Feature(
            name="lower_name", dtype=String, transform=PythonUdfTransform(name_to_lower)
        )

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=3),
                group_by_keys=["lower_name"],
                step_size=timedelta(days=1),
            ),
        )

        f_total_cost_sqrt = Feature(
            name="total_cost_sqrt",
            dtype=Float64,
            transform=PythonUdfTransform(lambda row: sqrt(row["total_cost"])),
        )

        expected_results = [
            (
                ENABLE_EMPTY_WINDOW_OUTPUT_SKIP_SAME_WINDOW_OUTPUT,
                pd.DataFrame(
                    [
                        [
                            to_epoch_millis("2022-01-01 23:59:59.999"),
                            "alex",
                            100,
                            sqrt(100),
                        ],
                        [
                            to_epoch_millis("2022-01-02 23:59:59.999"),
                            "alex",
                            400,
                            sqrt(400),
                        ],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "alex",
                            1000,
                            sqrt(1000),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "alex",
                            900,
                            sqrt(900),
                        ],
                        [
                            to_epoch_millis("2022-01-05 23:59:59.999"),
                            "alex",
                            600,
                            sqrt(600),
                        ],
                        [to_epoch_millis("2022-01-06 23:59:59.999"), "alex", 0, 0],
                        [
                            to_epoch_millis("2022-01-01 23:59:59.999"),
                            "emma",
                            400,
                            sqrt(400),
                        ],
                        [
                            to_epoch_millis("2022-01-02 23:59:59.999"),
                            "emma",
                            600,
                            sqrt(600),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "emma",
                            200,
                            sqrt(200),
                        ],
                        [to_epoch_millis("2022-01-05 23:59:59.999"), "emma", 0, 0],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "jack",
                            500,
                            sqrt(500),
                        ],
                        [to_epoch_millis("2022-01-06 23:59:59.999"), "jack", 0, 0],
                    ],
                    columns=[
                        "window_time",
                        "lower_name",
                        "total_cost",
                        "total_cost_sqrt",
                    ],
                ),
            ),
            (
                DISABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
                pd.DataFrame(
                    [
                        [
                            to_epoch_millis("2022-01-01 23:59:59.999"),
                            "alex",
                            100,
                            sqrt(100),
                        ],
                        [
                            to_epoch_millis("2022-01-02 23:59:59.999"),
                            "alex",
                            400,
                            sqrt(400),
                        ],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "alex",
                            1000,
                            sqrt(1000),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "alex",
                            900,
                            sqrt(900),
                        ],
                        [
                            to_epoch_millis("2022-01-05 23:59:59.999"),
                            "alex",
                            600,
                            sqrt(600),
                        ],
                        [
                            to_epoch_millis("2022-01-01 23:59:59.999"),
                            "emma",
                            400,
                            sqrt(400),
                        ],
                        [
                            to_epoch_millis("2022-01-02 23:59:59.999"),
                            "emma",
                            600,
                            sqrt(600),
                        ],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "emma",
                            600,
                            sqrt(600),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "emma",
                            200,
                            sqrt(200),
                        ],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "jack",
                            500,
                            sqrt(500),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "jack",
                            500,
                            sqrt(500),
                        ],
                        [
                            to_epoch_millis("2022-01-05 23:59:59.999"),
                            "jack",
                            500,
                            sqrt(500),
                        ],
                    ],
                    columns=[
                        "window_time",
                        "lower_name",
                        "total_cost",
                        "total_cost_sqrt",
                    ],
                ),
            ),
            (
                ENABLE_EMPTY_WINDOW_OUTPUT_WITHOUT_SKIP_SAME_WINDOW_OUTPUT,
                pd.DataFrame(
                    [
                        [
                            to_epoch_millis("2022-01-01 23:59:59.999"),
                            "alex",
                            100,
                            sqrt(100),
                        ],
                        [
                            to_epoch_millis("2022-01-02 23:59:59.999"),
                            "alex",
                            400,
                            sqrt(400),
                        ],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "alex",
                            1000,
                            sqrt(1000),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "alex",
                            900,
                            sqrt(900),
                        ],
                        [
                            to_epoch_millis("2022-01-05 23:59:59.999"),
                            "alex",
                            600,
                            sqrt(600),
                        ],
                        [to_epoch_millis("2022-01-06 23:59:59.999"), "alex", 0, 0],
                        [
                            to_epoch_millis("2022-01-01 23:59:59.999"),
                            "emma",
                            400,
                            sqrt(400),
                        ],
                        [
                            to_epoch_millis("2022-01-02 23:59:59.999"),
                            "emma",
                            600,
                            sqrt(600),
                        ],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "emma",
                            600,
                            sqrt(600),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "emma",
                            200,
                            sqrt(200),
                        ],
                        [to_epoch_millis("2022-01-05 23:59:59.999"), "emma", 0, 0],
                        [
                            to_epoch_millis("2022-01-03 23:59:59.999"),
                            "jack",
                            500,
                            sqrt(500),
                        ],
                        [
                            to_epoch_millis("2022-01-04 23:59:59.999"),
                            "jack",
                            500,
                            sqrt(500),
                        ],
                        [
                            to_epoch_millis("2022-01-05 23:59:59.999"),
                            "jack",
                            500,
                            sqrt(500),
                        ],
                        [to_epoch_millis("2022-01-06 23:59:59.999"), "jack", 0, 0],
                    ],
                    columns=[
                        "window_time",
                        "lower_name",
                        "total_cost",
                        "total_cost_sqrt",
                    ],
                ),
            ),
        ]

        for props, expected_result_df in expected_results:
            features = SlidingFeatureView(
                name="features",
                source=source,
                features=[f_lower_name, f_total_cost, f_total_cost_sqrt],
                props=props,
            )

            expected_result_df = expected_result_df.sort_values(
                by=["lower_name", "window_time"]
            ).reset_index(drop=True)

            result_df = (
                self.client.get_features(features)
                .to_pandas()
                .sort_values(by=["lower_name", "window_time"])
                .reset_index(drop=True)
            )
            self.assertTrue(
                expected_result_df.equals(result_df),
                f"Failed with props: {props}\nexpected: {expected_result_df}\n"
                f"actual: {result_df}",
            )

    def test_over_window_on_join_field(self):
        df_1 = self.input_data.copy()
        source = self._create_file_source(df_1)

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01,09:01:00"],
                ["Emma", 400.0, "2022-01-01,09:02:00"],
                ["Alex", 200.0, "2022-01-02,09:03:00"],
                ["Emma", 300.0, "2022-01-02,09:04:00"],
                ["Jack", 500.0, "2022-01-03,09:05:00"],
                ["Alex", 450.0, "2022-01-03,09:06:00"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self._create_file_source(
            df_2,
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
            timestamp_format="%Y-%m-%d,%H:%M:%S",
            keys=["name"],
        )
        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=source,
            features=[
                Feature(
                    name="cost",
                    dtype=Int64,
                    transform="cost",
                ),
                "distance",
                f"{source_2.name}.avg_cost",
                Feature(
                    name="derived_cost",
                    dtype=Float64,
                    transform="avg_cost * distance",
                ),
                Feature(
                    name="last_avg_cost",
                    dtype=Int64,
                    transform=OverWindowTransform(
                        expr="avg_cost",
                        agg_func="LAST_VALUE",
                        window_size=timedelta(days=2),
                        group_by_keys=["name"],
                        limit=2,
                    ),
                ),
                Feature(
                    name="double_last_avg_cost",
                    dtype=Float64,
                    transform="last_avg_cost * 2",
                ),
            ],
            keep_source_fields=False,
        )

        [_, built_feature_view_2] = self.client.build_features(
            [source_2, feature_view_2]
        )

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 20000.0, 100000.0, None, 160000.0]
        )
        expected_result_df["last_avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["double_last_avg_cost"] = pd.Series(
            [None, None, 200.0, 800.0, None, 400.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=built_feature_view_2)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_expression_transform_on_joined_field(self):
        df_1 = self.input_data.copy()
        source = self._create_file_source(df_1)

        df_2 = pd.DataFrame(
            [
                ["Alex", 100.0, "2022-01-01,09:01:00"],
                ["Emma", 400.0, "2022-01-01,09:02:00"],
                ["Alex", 200.0, "2022-01-02,09:03:00"],
                ["Emma", 300.0, "2022-01-02,09:04:00"],
                ["Jack", 500.0, "2022-01-03,09:05:00"],
                ["Alex", 450.0, "2022-01-03,09:06:00"],
            ],
            columns=["name", "avg_cost", "time"],
        )
        source_2 = self._create_file_source(
            df_2,
            schema=Schema(["name", "avg_cost", "time"], [String, Float64, String]),
            timestamp_format="%Y-%m-%d,%H:%M:%S",
            keys=["name"],
        )
        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=source,
            features=[
                Feature(
                    name="cost",
                    dtype=Int64,
                    transform="cost",
                ),
                "distance",
                f"{source_2.name}.avg_cost",
                Feature(
                    name="derived_cost",
                    dtype=Float64,
                    transform="avg_cost * distance",
                ),
            ],
            keep_source_fields=False,
        )

        [_, built_feature_view_2] = self.client.build_features(
            [source_2, feature_view_2]
        )

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [None, None, 100.0, 400.0, None, 200.0]
        )
        expected_result_df["derived_cost"] = pd.Series(
            [None, None, 20000.0, 100000.0, None, 160000.0]
        )
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=built_feature_view_2)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertListEqual(["name"], built_feature_view_2.keys)
        self.assertTrue(expected_result_df.equals(result_df))

    def test_expression_transform_on_over_window_transform(self):
        df, schema = self._create_input_data_and_schema_with_millis_time_span()

        source = self._create_file_source(
            df, timestamp_format="%Y-%m-%d %H:%M:%S.%f", schema=schema
        )

        features = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="cost_sum",
                    dtype=Int64,
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="SUM",
                        group_by_keys=["name"],
                        window_size=timedelta(milliseconds=3),
                    ),
                ),
                Feature(name="double_cost_sum", dtype=Int64, transform="cost_sum * 2"),
            ],
        )

        expected_result_df = df
        expected_result_df["cost_sum"] = pd.Series([100, 400, 400, 600, 500, 900])
        expected_result_df["double_cost_sum"] = pd.Series(
            [200, 800, 800, 1200, 1000, 1800]
        )
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features(features=features)
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )
        self.assertTrue(expected_result_df.equals(result_df))

    def test_expression_transform_and_over_window_transform(self):
        df_1 = self.input_data.copy()
        source = self._create_file_source(df_1)

        feature_view_1 = DerivedFeatureView(
            name="feature_view_1",
            source=source,
            features=[
                Feature(
                    "avg_cost",
                    dtype=Float64,
                    transform=OverWindowTransform(
                        expr="cost",
                        agg_func="AVG",
                        group_by_keys=["name"],
                        window_size=timedelta(days=2),
                    ),
                ),
                Feature("10_times_cost", dtype=Int64, transform="10 * cost"),
            ],
            keep_source_fields=True,
        )

        feature_view_2 = DerivedFeatureView(
            name="feature_view_2",
            source=feature_view_1,
            features=["avg_cost", "10_times_cost"],
        )

        self.client.build_features([feature_view_1, feature_view_2])

        expected_result_df = df_1
        expected_result_df["avg_cost"] = pd.Series(
            [100.0, 400.0, 200.0, 300.0, 500.0, 450.0]
        )
        expected_result_df["10_times_cost"] = pd.Series(
            [1000, 4000, 3000, 2000, 5000, 6000]
        )
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        result_df = (
            self.client.get_features("feature_view_2")
            .to_pandas()
            .sort_values(by=["name", "time"])
            .reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))
