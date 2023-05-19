#  Copyright 2022 The FeatHub Authors
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
import unittest
from abc import ABC
from datetime import timedelta
from typing import cast

import pandas as pd

from feathub.common import types
from feathub.common.exceptions import FeathubException, FeathubConfigurationException
from feathub.common.test_utils import to_epoch_millis
from feathub.common.types import Int64
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.feature import Feature
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
    WINDOW_TIME_EXPR,
)
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.registries.local_registry import LocalRegistry
from feathub.table.schema import Schema
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class SlidingFeatureViewTest(unittest.TestCase):
    def setUp(self):
        self.registry = LocalRegistry(props={})
        self.source = FileSystemSource(
            name="source_1",
            path="dummy_source_file",
            data_format="csv",
            schema=Schema(
                ["id", "id2", "fare_amount", "distance", "lpep_dropoff_datetime"],
                [types.Int32, types.Int32, types.Int32, types.Int32, types.Int64],
            ),
            timestamp_field="lpep_dropoff_datetime",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )

    def test_features(self):
        feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform="id + 1",
        )

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["feature_1"],
                step_size=timedelta(seconds=10),
            ),
        )

        feature_view_1 = SlidingFeatureView(
            name="feature_view_1",
            source=self.source,
            features=[
                feature_1,
                feature_2,
            ],
        )

        built_feature_view = self.registry.build_features([feature_view_1])[0]

        self.assertEqual(feature_1, built_feature_view.get_feature("feature_1"))
        self.assertEqual(feature_2, built_feature_view.get_feature("feature_2"))

    def test_join_transform(self):
        feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform=JoinTransform(table_name="t1", feature_name="f1"),
        )

        with self.assertRaises(FeathubException):
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    feature_1,
                ],
            ).build(self.registry)

    def test_expression_transform_not_grouping_key(self):
        feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform="id + 1",
        )

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        with self.assertRaises(FeathubException) as cm:
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    feature_1,
                    feature_2,
                ],
            ).build(self.registry)
        self.assertIn("not used as grouping key", cm.exception.args[0])

    def test_sliding_with_expression_transform(self):
        feature_1 = Feature(
            name="feature_1",
            transform="id + 1",
        )

        sliding_feature_1 = Feature(
            name="sliding_feature_1",
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id", "feature_1"],
                step_size=timedelta(seconds=10),
            ),
        )

        after_sliding_feature = Feature(
            name="after_sliding_feature",
            transform="sliding_feature_1 + id + window_time",
        )

        features = [
            feature_1,
            sliding_feature_1,
            after_sliding_feature,
        ]
        feature_view = SlidingFeatureView(
            name="feature_view_1",
            source=self.source,
            features=features,
        )

        built_feature_view = feature_view.build(self.registry)

        self.assertTrue(
            set([f.name for f in features]).issubset(
                set([f.name for f in built_feature_view.get_output_features()])
            )
        )

    def test_expression_transform_not_depends_on_sliding_window(self):
        sliding_feature = Feature(
            name="sliding_feature",
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        after_sliding_feature = Feature(
            name="after_sliding_feature", transform="fare_amount * 2"
        )

        with self.assertRaises(FeathubException) as cm:
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    sliding_feature,
                    after_sliding_feature,
                ],
            ).build(self.registry)

        self.assertIn(
            "after sliding window should only depend on timestamp field, "
            "sliding window features, or group-by keys.",
            cm.exception.args[0],
        )

    def test_str_feature_not_grouping_key(self):
        feature_1 = "distance"

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        with self.assertRaises(FeathubException) as cm:
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    feature_1,
                    feature_2,
                ],
            ).build(self.registry)
        self.assertIn("not used as grouping key", cm.exception.args[0])

    def test_different_group_by_keys(self):
        feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT)",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id2"],
                step_size=timedelta(seconds=10),
            ),
        )

        with self.assertRaises(FeathubException) as cm:
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    feature_1,
                    feature_2,
                ],
            ).build(self.registry)
        self.assertIn("different group-by keys", cm.exception.args[0])

    def test_different_step(self):
        feature_1 = Feature(
            name="feature_1",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT)",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=11),
            ),
        )

        with self.assertRaises(FeathubException) as cm:
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    feature_1,
                    feature_2,
                ],
            ).build(self.registry)
        self.assertIn("different step size", cm.exception.args[0])

    def test_without_sliding_window_transform(self):
        feature_1 = "fare_amount"

        feature_2 = Feature(
            name="feature_2",
            dtype=types.Float32,
            transform="fare_amount + 1",
        )

        with self.assertRaises(FeathubException) as cm:
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    feature_1,
                    feature_2,
                ],
            ).build(self.registry)

        self.assertIn(
            "at least one feature with SlidingWindowTransform", cm.exception.args[0]
        )

    def test_sliding_feature_view_window_timestamp(self):
        feature = Feature(
            name="feature",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        feature_view = SlidingFeatureView(
            name="feature_view_1",
            source=self.source,
            features=[
                feature,
            ],
        )

        expected_timestamp_feature = Feature(
            name="window_time",
            dtype=types.Int64,
            transform=WINDOW_TIME_EXPR,
            keys=["id"],
        )

        self.assertEqual(
            expected_timestamp_feature, feature_view.get_feature("window_time")
        )

        feature_view = SlidingFeatureView(
            name="feature_view_1",
            source=self.source,
            features=[
                feature,
            ],
            timestamp_field="my_window_time_field",
        )

        expected_timestamp_feature = Feature(
            name="my_window_time_field",
            dtype=types.Int64,
            transform=WINDOW_TIME_EXPR,
            keys=["id"],
        )

        self.assertEqual(
            expected_timestamp_feature, feature_view.get_feature("my_window_time_field")
        )

        feature_view = SlidingFeatureView(
            name="feature_view_1",
            source=self.source,
            features=[
                feature,
            ],
            timestamp_field="my_window_time_field",
        )

        expected_timestamp_feature = Feature(
            name="my_window_time_field",
            dtype=types.Int64,
            transform=WINDOW_TIME_EXPR,
            keys=["id"],
        )

        self.assertEqual("epoch_millis", feature_view.timestamp_format)
        self.assertEqual(
            expected_timestamp_feature, feature_view.get_feature("my_window_time_field")
        )

    def test_invalid_config(self):
        feature = Feature(
            name="feature",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        with self.assertRaises(FeathubConfigurationException) as cm:
            SlidingFeatureView(
                name="feature_view_1",
                source=self.source,
                features=[
                    feature,
                ],
                extra_props={
                    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: False,
                    SKIP_SAME_WINDOW_OUTPUT_CONFIG: True,
                },
            )
        self.assertIn("is forbidden", cm.exception.args[0])

    def test_build_with_config(self):
        feature = Feature(
            name="feature",
            dtype=types.Float32,
            transform=SlidingWindowTransform(
                expr="CAST(fare_amount AS FLOAT) + 1",
                agg_func="SUM",
                window_size=timedelta(seconds=30),
                group_by_keys=["id"],
                step_size=timedelta(seconds=10),
            ),
        )

        features = SlidingFeatureView(
            name="feature_view_1",
            source=self.source,
            features=[
                feature,
            ],
            extra_props={
                ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: False,
                SKIP_SAME_WINDOW_OUTPUT_CONFIG: False,
            },
        )

        built_feature = cast(
            SlidingFeatureView,
            features.build(
                self.registry,
                props={
                    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: True,
                    SKIP_SAME_WINDOW_OUTPUT_CONFIG: False,
                },
            ),
        )

        self.assertFalse(built_feature.config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG))
        self.assertFalse(built_feature.config.get(SKIP_SAME_WINDOW_OUTPUT_CONFIG))

        features = SlidingFeatureView(
            name="feature_view_1",
            source=self.source,
            features=[
                feature,
            ],
        )

        built_feature = cast(
            SlidingFeatureView,
            features.build(
                self.registry,
                props={
                    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: True,
                    SKIP_SAME_WINDOW_OUTPUT_CONFIG: False,
                },
            ),
        )

        self.assertTrue(built_feature.config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG))
        self.assertFalse(built_feature.config.get(SKIP_SAME_WINDOW_OUTPUT_CONFIG))


class SlidingFeatureViewITTest(ABC, FeathubITTestBase):
    def test_sliding_feature_view_filter_expr(self):
        df = self.input_data.copy()
        source = self.create_file_source(df)

        f_total_cost = Feature(
            name="total_cost",
            dtype=Int64,
            transform=SlidingWindowTransform(
                expr="cost",
                agg_func="SUM",
                window_size=timedelta(days=2),
                step_size=timedelta(days=1),
            ),
        )

        features = SlidingFeatureView(
            name="features",
            source=source,
            features=[f_total_cost],
            filter_expr="total_cost > 600",
            extra_props={
                ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: True,
                SKIP_SAME_WINDOW_OUTPUT_CONFIG: True,
            },
        )

        result_df = (
            self.client.get_features(feature_descriptor=features)
            .to_pandas()
            .sort_values(by=["window_time"])
            .reset_index(drop=True)
        )

        expected_result_df = pd.DataFrame(
            [
                [to_epoch_millis("2022-01-02 23:59:59.999"), 1000],
                [to_epoch_millis("2022-01-03 23:59:59.999"), 1600],
                [to_epoch_millis("2022-01-04 23:59:59.999"), 1100],
            ],
            columns=["window_time", "total_cost"],
        )

        self.assertTrue(expected_result_df.equals(result_df))
