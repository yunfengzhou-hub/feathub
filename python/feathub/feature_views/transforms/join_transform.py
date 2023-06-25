# Copyright 2022 The FeatHub Authors
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
from typing import Dict

from feathub.common.utils import append_metadata_to_json
from feathub.dsl.ast import VariableNode
from feathub.dsl.expr_parser import ExprParser
from feathub.feature_views.transforms.transformation import Transformation


_parser = ExprParser()


class JoinTransform(Transformation):
    """
    Derives feature values by joining parent table with a feature from another table.
    """

    def __init__(
        self,
        table_name: str,
        feature_expr: str,
    ):
        """
        :param table_name: The name of a Source or FeatureView table.
        :param feature_expr: The feature expr.
        """
        super().__init__()
        self.table_name = table_name
        self.feature_expr = feature_expr

    # TODO: remove this method after Feathub expression syntax on JoinTransform is
    #  supported by local/spark processor and online feature service.
    def expr_is_feature_name(self) -> bool:
        """
        Returns whether the feature expression of this transform is a single
        feature name.
        """
        return isinstance(_parser.parse(self.feature_expr), VariableNode)

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "table_name": self.table_name,
            "feature_expr": self.feature_expr,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "JoinTransform":
        return JoinTransform(
            table_name=json_dict["table_name"],
            feature_expr=json_dict["feature_expr"],
        )
