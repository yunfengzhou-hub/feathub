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
from typing import Optional, Sequence, List, Dict

from pyspark.sql import DataFrame as NativeSparkDataFrame, Column, functions
from pyspark.sql.functions import window

from feathub.common.exceptions import FeathubTransformationException
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.sliding_window_transform import SlidingWindowTransform
from feathub.processors.flink.table_builder.flink_sql_expr_utils import to_flink_sql_expr
from feathub.processors.spark.dataframe_builder.aggregation_utils import (
    AggregationFieldDescriptor,
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


def evaluate_sliding_window_transform(
    dataframe: NativeSparkDataFrame,
    window_descriptor: "SlidingWindowDescriptor",
    agg_descriptors: List["AggregationFieldDescriptor"],
) -> NativeSparkDataFrame:
    agg_columns = _get_over_window_agg_columns(agg_descriptors)

    return (
        dataframe.groupby(
            *window_descriptor.group_by_keys,
            window(dataframe.timestamp, "10 minutes", "5 minutes").alias("window_time"),
        )
        .agg(*agg_columns.values())
        .select(
            *window_descriptor.group_by_keys,
            "window_time",
            *[x for x in agg_columns.keys()]
        )
    )


def _get_over_window_agg_columns(
    agg_descriptors: List["AggregationFieldDescriptor"],
) -> Dict[str, Column]:
    return {
        descriptor.field_name: _get_over_window_agg_column(
            descriptor.expr,
            descriptor.agg_func,
        )
        .cast(descriptor.field_data_type)
        .alias(descriptor.field_name)
        for descriptor in agg_descriptors
    }


def _get_over_window_agg_column(
    expression: str,
    agg_func: AggFunc,
) -> Column:
    if agg_func == AggFunc.AVG:
        result = functions.expr(f"avg({expression})")
    elif agg_func == AggFunc.MIN:
        result = functions.expr(f"min({expression})")
    elif agg_func == AggFunc.MAX:
        result = functions.expr(f"max({expression})")
    elif agg_func == AggFunc.SUM:
        result = functions.expr(f"sum({expression})")
    elif agg_func == AggFunc.FIRST_VALUE:
        result = functions.expr(f"first_value({expression})")
    elif agg_func == AggFunc.LAST_VALUE:
        result = functions.expr(f"last_value({expression})")
    elif agg_func == AggFunc.ROW_NUMBER:
        result = functions.row_number()
    elif agg_func == AggFunc.COUNT:
        result = functions.expr(f"count({expression})")
    elif agg_func == AggFunc.VALUE_COUNTS:
        # TODO Adds VALUE_COUNTS support for SparkProcessor
        raise FeathubTransformationException(
            "VALUE_COUNTS is not supported for SparkProcessor currently."
        )
    else:
        raise FeathubTransformationException(
            f"Unsupported aggregation for SparkProcessor {agg_func}."
        )

    return result
