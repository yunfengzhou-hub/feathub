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

from feathub.feature_views.transforms.transformation import Transformation


class ExpressionTransform(Transformation):
    """
    Derives feature values by applying FeatHub expression on one row of the parent table
    at a time. FeatHub expression is a declarative language with built-in functions.
    """

    def __init__(self, expr: str) -> None:
        """
        :param expr: A FeatHub expression composed of UDF and feature names.
        """
        super().__init__()
        self.expr = expr

    def to_json(self) -> Dict:
        return {"type": "ExpressionTransform", "expr": self.expr}

    @classmethod
    def from_json(cls, json_dict: Dict) -> "ExpressionTransform":
        return ExpressionTransform(json_dict["expr"])
