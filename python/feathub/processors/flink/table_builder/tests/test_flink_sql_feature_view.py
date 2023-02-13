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
from datetime import timedelta
from typing import Union, List, Optional

import pandas as pd

from feathub.common import types
from feathub.common.test_utils import to_epoch_millis
from feathub.common.types import Int64
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureView,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.sql_feature_view import SqlFeatureView
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class FlinkSqlFeatureViewITTest(ABC, FeathubITTestBase):
    def setUp(self) -> None:
        FeathubITTestBase.setUp(self)
        self.client = self.get_client({"test_id": self.id()})

    def tearDown(self) -> None:
        FeathubITTestBase.tearDown(self)
        self.client = self.get_client()

    def test_select(self):
        source = self.create_file_source(self.input_data.copy(), name="__SOURCE__")

        self._test_sql_statement(
            sources=[source],
            sql_statement="""
                CREATE VIEW test_view_name AS (
                    SELECT name, cost FROM __SOURCE__
                );
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .build()
            ),
            expected_result_df=pd.DataFrame(
                [
                    ["Alex", 100],
                    ["Emma", 400],
                    ["Alex", 300],
                    ["Emma", 200],
                    ["Jack", 500],
                    ["Alex", 600],
                ],
                columns=["name", "cost"],
            ),
        )

    def test_flink_udf(self):
        source = self.create_file_source(self.input_data.copy(), name="__SOURCE__")

        self._test_sql_statement(
            sources=[source],
            sql_statement="""
                CREATE VIEW test_view_name AS (
                    SELECT *, UPPER(name) as upper_name FROM __SOURCE__
                );
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .column("distance", types.Int64)
                .column("time", types.String)
                .column("upper_name", types.String)
                .build()
            ),
            expected_result_df=pd.DataFrame(
                [
                    ["Alex", 100, 100, "2022-01-01 08:01:00", "ALEX"],
                    ["Emma", 400, 250, "2022-01-01 08:02:00", "EMMA"],
                    ["Alex", 300, 200, "2022-01-02 08:03:00", "ALEX"],
                    ["Emma", 200, 250, "2022-01-02 08:04:00", "EMMA"],
                    ["Jack", 500, 500, "2022-01-03 08:05:00", "JACK"],
                    ["Alex", 600, 800, "2022-01-03 08:06:00", "ALEX"],
                ],
                columns=["name", "cost", "distance", "time", "upper_name"],
            ),
        )

    def test_timestamp_watermark(self):
        source = self.create_file_source(self.input_data.copy(), name="__SOURCE__")

        features: FeatureView = SqlFeatureView(
            name="test_view_name",
            sources=[source],
            sql_statement="""
                CREATE VIEW test_view_name AS (
                    SELECT name, cost, `time`, __event_time_attribute__ FROM __SOURCE__
                );
            """,
            schema=(
                Schema.new_builder()
                .column("name", types.String)
                .column("cost", types.Int64)
                .column("time", types.String)
                .build()
            ),
            timestamp_field="time",
            timestamp_format="epoch",
        )

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
            source=features,
            features=[f_total_cost],
            props={
                ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG: True,
                SKIP_SAME_WINDOW_OUTPUT_CONFIG: True,
            },
        )

        result_df = self.client.get_features(features).to_pandas()

        expected_result_df = pd.DataFrame(
            [
                [to_epoch_millis("2022-01-01 23:59:59.999"), 500],
                [to_epoch_millis("2022-01-02 23:59:59.999"), 1000],
                [to_epoch_millis("2022-01-03 23:59:59.999"), 1600],
                [to_epoch_millis("2022-01-04 23:59:59.999"), 1100],
                [to_epoch_millis("2022-01-05 23:59:59.999"), 0],
            ],
            columns=["window_time", "total_cost"],
        )

        self.assertTrue(expected_result_df.equals(result_df))

    def _test_sql_statement(
        self,
        sources: List[Union[str, TableDescriptor]],
        sql_statement: str,
        schema: Schema,
        expected_result_df: pd.DataFrame,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
    ):
        features = SqlFeatureView(
            name="test_view_name",
            sources=sources,
            sql_statement=sql_statement,
            schema=schema,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )

        result_df = self.client.get_features(features).to_pandas()

        self.assertTrue(expected_result_df.equals(result_df))
