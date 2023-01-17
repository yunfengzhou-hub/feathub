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

import pandas as pd

from feathub.common.types import String
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.python_udf_transform import PythonUdfTransform
from feathub.tests.feathub_it_test_base import FeathubITTestBase


class PythonUDFTransformITTest(ABC, FeathubITTestBase):
    def test_python_udf_transform(self):
        df_1 = self.input_data.copy()
        source = self.create_file_source(df_1)

        def name_to_lower(row: pd.Series) -> str:
            return row["name"].lower()

        feature_view = DerivedFeatureView(
            name="feature_view",
            source=source,
            features=[
                Feature(
                    name="lower_name",
                    dtype=String,
                    transform=PythonUdfTransform(name_to_lower),
                    keys=["name"],
                )
            ],
        )

        expected_result_df = df_1
        expected_result_df["lower_name"] = expected_result_df["name"].apply(
            lambda name: name.lower()
        )
        expected_result_df.drop(["cost", "distance"], axis=1, inplace=True)
        expected_result_df = expected_result_df.sort_values(
            by=["name", "time"]
        ).reset_index(drop=True)

        table = self.client.get_features(features=feature_view)
        result_df = (
            table.to_pandas().sort_values(by=["name", "time"]).reset_index(drop=True)
        )

        self.assertTrue(expected_result_df.equals(result_df))
