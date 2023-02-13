# Copyright 2022 The Feathub Authors
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
from typing import Sequence

from feathub.feature_views.feature import Feature
from feathub.table.table_descriptor import TableDescriptor


class FeatureView(TableDescriptor, ABC):
    """
    Provides metadata to derive a table of feature values from other tables.
    """

    @abstractmethod
    def is_unresolved(self) -> bool:
        pass

    @abstractmethod
    def get_resolved_features(self) -> Sequence[Feature]:
        pass
