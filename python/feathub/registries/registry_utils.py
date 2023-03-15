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
import typing

from feathub.common.exceptions import FeathubException
from feathub.registries.local_registry import LocalRegistry
from feathub.registries.registry import Registry


# TODO: Add public API to get all registered features in a Registry instance
#  and remove this utility function.
def get_all_registered_table_names(registry: Registry) -> typing.Set[str]:
    if isinstance(registry, LocalRegistry):
        return set(x for x in typing.cast(LocalRegistry, registry).tables.keys())

    raise FeathubException(f"Unsupported Registry type {type(registry)}.")
