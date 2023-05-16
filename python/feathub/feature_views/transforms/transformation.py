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

from abc import ABC, abstractmethod
import json
from typing import Dict

from feathub.common.exceptions import FeathubException


class Transformation(ABC):
    """
    Abstract class for classes that define how to derive a new feature from existing
    features.
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    def to_json(self) -> Dict:
        """
        Returns a json-formatted object representing this sink.
        """
        pass

    @classmethod
    def from_json(cls, json_dict: Dict):
        if json_dict["type"] == "ExpressionTransform":
            from feathub.feature_views.transforms.expression_transform import (
                ExpressionTransform,
            )

            return ExpressionTransform.from_json(json_dict)
        elif json_dict["type"] == "JoinTransform":
            from feathub.feature_views.transforms.join_transform import JoinTransform

            return JoinTransform.from_json(json_dict)
        elif json_dict["type"] == "OverWindowTransform":
            from feathub.feature_views.transforms.over_window_transform import (
                OverWindowTransform,
            )

            return OverWindowTransform.from_json(json_dict)
        elif json_dict["type"] == "PythonUdfTransform":
            from feathub.feature_views.transforms.python_udf_transform import (
                PythonUdfTransform,
            )

            return PythonUdfTransform.from_json(json_dict)
        elif json_dict["type"] == "SlidingWindowTransform":
            from feathub.feature_views.transforms.sliding_window_transform import (
                SlidingWindowTransform,
            )

            return SlidingWindowTransform.from_json(json_dict)

        raise FeathubException(f"Unsupported Transformation type {json_dict['type']}.")

    def __str__(self) -> str:
        return json.dumps(self.to_json(), indent=2, sort_keys=True)
