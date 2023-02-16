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
from datetime import timedelta
from typing import List, Optional, Sequence

from pyflink.java_gateway import get_gateway
from pyflink.table import (
    Table as NativeFlinkTable,
    expressions as native_flink_expr,
)
from pyflink.table.types import _to_java_data_type

from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureViewConfig,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
    get_default_value_and_type,
)
from feathub.processors.flink.table_builder.flink_sql_expr_utils import (
    to_flink_sql_expr,
)


class SlidingWindowDescriptor:
    """
    Descriptor of a sliding window.
    """

    def __init__(
        self,
        step_size: timedelta,
        limit: Optional[int],
        group_by_keys: Sequence[str],
        filter_expr: Optional[str],
    ) -> None:
        self.step_size = step_size
        self.limit = limit
        self.group_by_keys = group_by_keys
        self.filter_expr = filter_expr

    @staticmethod
    def from_sliding_window_transform(
        sliding_window_agg: SlidingWindowTransform,
    ) -> "SlidingWindowDescriptor":
        filter_expr = (
            to_flink_sql_expr(sliding_window_agg.filter_expr)
            if sliding_window_agg.filter_expr is not None
            else None
        )
        return SlidingWindowDescriptor(
            sliding_window_agg.step_size,
            sliding_window_agg.limit,
            sliding_window_agg.group_by_keys,
            filter_expr,
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.step_size == other.step_size
            and self.limit == other.limit
            and self.group_by_keys == other.group_by_keys
            and self.filter_expr == other.filter_expr
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.step_size,
                self.limit,
                tuple(self.group_by_keys),
                self.filter_expr,
            )
        )


# TODO: Retracting the value when the window becomes empty when the Sink support
#  DynamicTable with retraction.
def evaluate_sliding_window_transform(
    flink_table: NativeFlinkTable,
    window_descriptor: SlidingWindowDescriptor,
    agg_descriptors: List[AggregationFieldDescriptor],
    config: SlidingFeatureViewConfig,
) -> NativeFlinkTable:
    """
    Evaluate the sliding window transforms on the given flink table and return the
    result table.

    :param flink_table: The input Flink table.
    :param window_descriptor: The descriptor of the sliding window.
    :param agg_descriptors: A list of descriptor that descriptor the aggregation to
                            perform.
    :param config: The config of the SlidingFeatureView that the window_descriptor
                   belongs to.
    :return: The result table.
    """
    if window_descriptor.filter_expr is not None:
        flink_table = flink_table.filter(
            native_flink_expr.call_sql(window_descriptor.filter_expr)
        )

    for agg_descriptor in agg_descriptors:
        flink_table = flink_table.add_or_replace_columns(
            native_flink_expr.call_sql(agg_descriptor.expr).alias(
                agg_descriptor.field_name
            )
        )

    gateway = get_gateway()
    group_by_keys = gateway.new_array(
        gateway.jvm.String, len(window_descriptor.group_by_keys)
    )
    for idx, key in enumerate(window_descriptor.group_by_keys):
        group_by_keys[idx] = key

    descriptor_builder = (
        gateway.jvm.com.alibaba.feathub.flink.udf.AggregationFieldsDescriptor.builder()
    )

    table_schema = flink_table.get_schema()

    for agg_descriptor in agg_descriptors:
        agg_func_name = agg_descriptor.agg_func.name
        descriptor_builder.addField(
            agg_descriptor.field_name,
            _to_java_data_type(
                table_schema.get_field_data_type(agg_descriptor.field_name)
            ),
            agg_descriptor.field_name,
            _to_java_data_type(agg_descriptor.field_data_type),
            int(agg_descriptor.window_size.total_seconds() * 1000),
            agg_func_name,
            window_descriptor.limit,
        )

    default_row = None
    if config.get(ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG):
        default_row = gateway.jvm.org.apache.flink.types.Row.withNames()
        for agg_descriptor in agg_descriptors:
            default_value, data_type = get_default_value_and_type(agg_descriptor)
            default_row.setField(agg_descriptor.field_name, default_value)

    j_table = gateway.jvm.com.alibaba.feathub.flink.udf.SlidingWindowUtils.applySlidingWindow(  # noqa
        flink_table._t_env._j_tenv,
        flink_table._j_table,
        group_by_keys,
        EVENT_TIME_ATTRIBUTE_NAME,
        int(window_descriptor.step_size.total_seconds() * 1000),
        descriptor_builder.build(),
        default_row,
        config.get(SKIP_SAME_WINDOW_OUTPUT_CONFIG),
    )
    return NativeFlinkTable(j_table, flink_table._t_env)
