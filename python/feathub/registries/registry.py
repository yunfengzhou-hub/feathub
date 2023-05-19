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

from __future__ import annotations
from typing import List, Dict, Optional
from abc import ABC, abstractmethod

from feathub.registries.registry_config import (
    RegistryConfig,
    REGISTRY_TYPE_CONFIG,
    RegistryType,
)
from feathub.table.table_descriptor import TableDescriptor


class Registry(ABC):
    """
    A registry implements APIs to build, register, get, and delete feature descriptors,
    such as feature views with feature transformation definitions.
    """

    def __init__(self, registry_type: str, config: Dict) -> None:
        """
        :param registry_type: The type of the registry
        :param config: The registry configuration.
        """
        self._registry_type = registry_type
        self._config = config

    @abstractmethod
    def build_features(
        self,
        feature_descriptor_list: List[TableDescriptor],
        props: Optional[Dict] = None,
    ) -> List[TableDescriptor]:
        """
        For each feature descriptor in the given list, resolve this descriptor by
        recursively replacing its dependent table and feature names with the
        corresponding feature descriptors and features from the cache or registry.
        Then recursively configure the feature descriptor and its dependent table that
        is referred by a TableDescriptor with the given global properties if it is not
        configured already.

        And caches the resolved descriptors as well as their dependent feature
        descriptors in memory so that they can be used when building other feature
        descriptors.

        :param feature_descriptor_list: A list of feature descriptors.
        :param props: Optional. If it is not None, it is the global properties that are
                      used to configure the given feature descriptors.
        :return: A list of resolved descriptors corresponding to the input descriptors.
        """
        pass

    @abstractmethod
    def register_features(
        self, feature_descriptor_list: List[TableDescriptor], override: bool = True
    ) -> bool:
        """
        Registers the given feature descriptors in the registry after building and
        caching them in memory as described in build_features. Each descriptor is
        uniquely identified by its name in the registry.

        :param feature_descriptor_list: A feature descriptor to be registered.
        :param override: Indicates whether the registration can overwrite existing
                         descriptor or not.
        :return: True iff registration is successful.
        """
        pass

    @abstractmethod
    def get_features(
        self, name: str, force_update: bool = False, is_built: bool = True
    ) -> TableDescriptor:
        """
        Returns the feature descriptor previously registered with the given name. Raises
        RuntimeError if there is no such table in the registry.

        :param name: The name of the feature descriptor to search for.
        :param force_update: If True, the feature descriptor would be directly searched
                             in registry. If False, the feature descriptor would be
                             searched in local cache first.
        :param is_built: If False, the original feature descriptor would be returned. If
                         True, the descriptor that had been built would be returned.
        :return: A feature descriptor with the given name.
        """
        pass

    @abstractmethod
    def delete_features(self, name: str) -> bool:
        """
        Deletes the feature descriptor with the given name from the registry.

        :param name: The name of the feature descriptor to be deleted.
        :return: True iff a feature descriptor with the given name is deleted.
        """
        pass

    @staticmethod
    def instantiate(props: Dict) -> Registry:
        """
        Instantiates a registry using the given properties.
        """

        registry_config = RegistryConfig(props)
        registry_type = RegistryType(registry_config.get(REGISTRY_TYPE_CONFIG))

        if registry_type == RegistryType.LOCAL:
            from feathub.registries.local_registry import LocalRegistry

            return LocalRegistry(props=props)

        elif registry_type == RegistryType.MYSQL:
            from feathub.registries.mysql_registry import MySqlRegistry

            return MySqlRegistry(props=props)

        raise RuntimeError(f"Failed to instantiate registry with props={props}.")

    @property
    def config(self) -> Dict:
        return self._config

    @property
    def registry_type(self) -> str:
        return self._registry_type

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self._config == other._config
            and self._registry_type == other._registry_type
        )
