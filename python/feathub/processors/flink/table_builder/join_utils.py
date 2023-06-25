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
from typing import List, Dict, Tuple, Any, Sequence, Optional, cast

from pyflink.table import (
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
    StreamTableEnvironment,
)
from pyflink.table.types import DataType

from feathub.feature_views.feature import Feature
from feathub.feature_views.transforms.join_transform import JoinTransform
from feathub.processors.constants import (
    EVENT_TIME_ATTRIBUTE_NAME,
    PROCESSING_TIME_ATTRIBUTE_NAME,
)
from feathub.processors.flink.flink_types_utils import to_flink_type
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    generate_random_table_name,
)


class JoinFieldDescriptor:
    """
    Descriptor of the join field.
    """

    def __init__(
        self,
        field_expr: str,
        field_data_type: Optional[DataType] = None,
    ):
        """
        :param field_expr: The expression of the field.
        :param field_data_type: Optional. If it is not None, the field is cast to the
                                given type. Otherwise, use its original type.
        """
        self.field_expr = field_expr
        self.field_data_type = field_data_type

    @staticmethod
    def from_feature(feature: Feature) -> "JoinFieldDescriptor":
        return JoinFieldDescriptor(
            cast(JoinTransform, feature.transform).feature_expr,
            to_flink_type(feature.dtype),
        )

    @staticmethod
    def from_field_name(field_name: str) -> "JoinFieldDescriptor":
        return JoinFieldDescriptor(field_name)

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.field_expr == other.field_expr
            and self.field_data_type == other.field_data_type
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.field_expr,
                self.field_data_type,
            )
        )


def join_table_on_key(
    left: NativeFlinkTable, right: NativeFlinkTable, key_fields: List[str]
) -> NativeFlinkTable:
    """
    Join the left and right table on the given key fields.
    """

    right_aliased = _rename_fields(right, key_fields)
    predicate = _get_join_predicate(left, right_aliased, key_fields)

    return left.join(right_aliased, predicate).select(
        *[
            native_flink_expr.col(left_field_name)
            for left_field_name in left.get_schema().get_field_names()
        ],
        *[
            native_flink_expr.col(right_field_name)
            for right_field_name in right.get_schema().get_field_names()
            if right_field_name not in key_fields
        ],
    )


def lookup_join(
    t_env: StreamTableEnvironment,
    left: NativeFlinkTable,
    right: NativeFlinkTable,
    key_fields: List[str],
) -> NativeFlinkTable:
    """
    Lookup join the right table to the left table.
    """
    left = _append_processing_time_attribute_if_not_exist(t_env, left)

    t_env.create_temporary_view("left_table", left)
    t_env.create_temporary_view("right_table", right)

    predicates = " and ".join(
        [f"left_table.`{k}` = right_table.`{k}`" for k in key_fields]
    )

    result_table = t_env.sql_query(
        f"""
        SELECT * FROM left_table LEFT JOIN right_table
            FOR SYSTEM_TIME AS OF left_table.`{PROCESSING_TIME_ATTRIBUTE_NAME}`
            ON {predicates};
        """
    )

    t_env.drop_temporary_view("left_table")
    t_env.drop_temporary_view("right_table")

    return result_table


# TODO: figure out why Flink SQL requires a processing time attribute for lookup join
#  and see if there is space for improvement on Flink API.
def _append_processing_time_attribute_if_not_exist(
    t_env: StreamTableEnvironment,
    table: NativeFlinkTable,
) -> NativeFlinkTable:
    if PROCESSING_TIME_ATTRIBUTE_NAME in table.get_schema().get_field_names():
        return table

    tmp_table_name = generate_random_table_name("tmp_table")

    t_env.create_temporary_view(tmp_table_name, table)

    table = t_env.sql_query(
        f"SELECT *, PROCTIME() AS {PROCESSING_TIME_ATTRIBUTE_NAME} "
        f"FROM {tmp_table_name};"
    )
    t_env.drop_temporary_view(tmp_table_name)

    return table


def full_outer_join_on_key_with_default_value(
    left: NativeFlinkTable,
    right: NativeFlinkTable,
    key_fields: List[str],
    field_default_values: Dict[str, Tuple[Any, DataType]],
) -> NativeFlinkTable:
    """
    Full outer join the left and right table on the given key fields. NULL fields after
    join are set to its default value.

    :param left: The left table.
    :param right: The right table.
    :param key_fields: The join keys.
    :param field_default_values: A map that map the field to its default value. If a
                                 field does not exist in the map, its default value is
                                 NULL.
    :return: The joined table.
    """
    right_aliased = _rename_fields(right, key_fields)
    predicate = _get_join_predicate(left, right_aliased, key_fields)

    return left.full_outer_join(right_aliased, predicate).select(
        *[
            native_flink_expr.if_then_else(
                native_flink_expr.col(key).is_not_null,
                native_flink_expr.col(key),
                native_flink_expr.col(f"right.{key}"),
            ).alias(key)
            for key in key_fields
        ],
        *[
            _field_with_default_value_if_null(left_field_name, field_default_values)
            for left_field_name in left.get_schema().get_field_names()
            if left_field_name not in key_fields
        ],
        *[
            _field_with_default_value_if_null(right_field_name, field_default_values)
            for right_field_name in right.get_schema().get_field_names()
            if right_field_name not in key_fields
        ],
    )


def temporal_join(
    t_env: StreamTableEnvironment,
    left: NativeFlinkTable,
    right: NativeFlinkTable,
    keys: Sequence[str],
    right_table_join_field_descriptor: Dict[str, JoinFieldDescriptor],
) -> NativeFlinkTable:
    """
    Temporal join the right table to the left table.

    :param t_env: The StreamTableEnvironment.
    :param left: The left table.
    :param right: The right table.
    :param keys: The join keys.
    :param right_table_join_field_descriptor: A map from right field name to its
                                              JoinFieldDescriptor.
    :return: The joined table.
    """
    t_env.create_temporary_view("left_table", left)
    t_env.create_temporary_view("right_table", right)
    escaped_keys = [f"`{k}`" for k in keys]
    temporal_right_table = t_env.sql_query(
        f"""
    SELECT * FROM (SELECT *,
        ROW_NUMBER() OVER (PARTITION BY {",".join(escaped_keys)}
            ORDER BY `{EVENT_TIME_ATTRIBUTE_NAME}` DESC) AS rownum
        FROM right_table)
    WHERE rownum = 1
    """
    )

    right_aliased = _rename_fields(
        temporal_right_table, [*keys, EVENT_TIME_ATTRIBUTE_NAME]
    )

    t_env.create_temporary_view("temporal_right_table", right_aliased)

    predicates = " and ".join(
        [f"left_table.`{k}` = temporal_right_table.`right.{k}`" for k in keys]
    )

    result_table = t_env.sql_query(
        f"""
    SELECT * FROM left_table LEFT JOIN
        temporal_right_table
        FOR SYSTEM_TIME AS OF left_table.`{EVENT_TIME_ATTRIBUTE_NAME}`
    ON {predicates}
    """
    )

    t_env.drop_temporary_view("left_table")
    t_env.drop_temporary_view("right_table")
    t_env.drop_temporary_view("temporal_right_table")
    return result_table


def _rename_fields(right: NativeFlinkTable, fields: Sequence[str]) -> NativeFlinkTable:
    aliased_right_table_field_names = [
        f"right.{f}" if f in fields else f for f in right.get_schema().get_field_names()
    ]
    right_aliased = right.alias(*aliased_right_table_field_names)
    return right_aliased


def _get_join_predicate(
    left: NativeFlinkTable, key_aliased_right: NativeFlinkTable, key_fields: List[str]
) -> native_flink_expr:
    predicates = [
        left.__getattr__(f"{key}") == key_aliased_right.__getattr__(f"right.{key}")
        for key in key_fields
    ]
    if len(predicates) > 1:
        predicate = native_flink_expr.and_(*predicates)
    else:
        predicate = predicates[0]
    return predicate


def _field_with_default_value_if_null(
    field_name: str, field_default_values: Dict[str, Tuple[Any, DataType]]
) -> native_flink_expr:
    if (
        field_name not in field_default_values
        or field_default_values[field_name][0] is None
    ):
        return native_flink_expr.col(field_name)

    return native_flink_expr.if_then_else(
        native_flink_expr.col(field_name).is_not_null,
        native_flink_expr.col(field_name),
        native_flink_expr.lit(field_default_values[field_name][0]).cast(
            field_default_values[field_name][1]
        ),
    ).alias(field_name)
