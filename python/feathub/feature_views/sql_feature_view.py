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
from typing import Dict, Sequence, Optional, List

from feathub.common.exceptions import FeathubException
from feathub.feature_views.feature import Feature
from feathub.feature_views.feature_view import FeatureView
from feathub.registries.registry import Registry
from feathub.registries.registry_utils import get_all_registered_table_names
from feathub.table.schema import Schema
from feathub.table.table_descriptor import TableDescriptor


class SqlFeatureView(FeatureView):
    """
    Acquires features by evaluating the given SQL query statement using a specific
    Processor.

    Apart from the following requirements, The SQL statement may use any sql
    syntax/function supported by the internal Processor, and it would not be checked
    against FeatHub grammars.

    - All the sources that the SQL statement would derive features from must have been
    defined and registered in FeatHub. The statement should not create new unregistered
    source tables on its own.
    - The SQL statement should be a query in the format `SELECT ... FROM xxx ...`, where
    `xxx` is the source that has been registered as described above.
    - The output table or view should contain all key fields if `keys` is not None.
    - If the FeatureView's timestamp_field is not None, which means at least one source
    table's timestamp_field is not None, the output table or view should contain the
    timestamp field and a column named `__event_time_attribute__` from the source table.

    Note that as the SQL statement is bound with a concrete Processor's syntax, FeatHub
    workflows containing this kind of FeatureView cannot be migrated between different
    Processors.
    """

    def __init__(
        self,
        name: str,
        sql_statement: str,
        schema: Schema,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        is_bounded: bool = True,
    ):
        """
        :param name: The unique identifier of this feature view in the registry.
        :param sql_statement: The SQL query statement to be evaluated with an internal
                              processor.
        :param schema: The schema of the output table.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field.
        :param is_bounded: Whether the output table of this SqlFeatureView is bounded.
        """
        TableDescriptor.__init__(
            self,
            name=name,
            keys=keys,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.sql_statement = sql_statement
        self.schema = schema

        # Names of the tables and views that might be the source of this feature view.
        self.possible_sources: Optional[Sequence[str]] = None

        if timestamp_field is not None and timestamp_field not in schema.field_names:
            raise FeathubException(
                f"Schema {schema} does not contain timestamp field {timestamp_field}."
            )

        if keys is not None and any(key not in schema.field_names for key in keys):
            raise FeathubException(f"Schema {schema} does not contain all keys {keys}.")

        self._is_bounded = is_bounded

    def build(
        self, registry: Registry, props: Optional[Dict] = None
    ) -> TableDescriptor:
        view = SqlFeatureView(
            name=self.name,
            sql_statement=self.sql_statement,
            schema=self.schema,
            keys=self.keys,
            timestamp_field=self.timestamp_field,
            timestamp_format=self.timestamp_format,
            is_bounded=self._is_bounded,
        )

        if self.possible_sources is None:
            view.possible_sources = [
                x for x in get_all_registered_table_names(registry) if x != self.name
            ]

            if not view.possible_sources:
                raise FeathubException(
                    f"No table or view has been built or registered prior to "
                    f"SqlFeatureView {self.name}."
                )
        else:
            view.possible_sources = self.possible_sources

        return view

    def is_unresolved(self) -> bool:
        return self.possible_sources is None

    def get_output_fields(self, source_fields: List[str]) -> List[str]:
        if self.is_unresolved():
            raise RuntimeError("Build this feature view before getting features.")
        return self.schema.field_names.copy()

    def get_feature(self, feature_name: str) -> Feature:
        if self.is_unresolved():
            raise RuntimeError("Build this feature view before getting features.")
        return Feature(
            name=feature_name,
            dtype=self.schema.get_field_type(feature_name),
            transform=feature_name,
            keys=self.keys,
        )

    def get_resolved_features(self) -> Sequence[Feature]:
        if self.is_unresolved():
            raise RuntimeError("Build this feature view before getting features.")
        return [self.get_feature(field_name) for field_name in self.schema.field_names]

    def get_resolved_source(self) -> TableDescriptor:
        raise FeathubException("Unsupported Operation.")

    def is_bounded(self) -> bool:
        return self._is_bounded

    def get_bounded_view(self) -> TableDescriptor:
        if not self._is_bounded:
            raise FeathubException(
                "SqlFeatureView is unbounded and it doesn't support getting bounded "
                "view."
            )
        return self

    def to_json(self) -> Dict:
        return_value = {
            "type": "SqlFeatureView",
            "name": self.name,
            "sql_statement": self.sql_statement,
            "schema": self.schema.to_json(),
            "keys": self.keys,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "is_bounded": self._is_bounded,
        }

        if self.possible_sources is not None:
            return_value["possible_sources"] = self.possible_sources

        return return_value
