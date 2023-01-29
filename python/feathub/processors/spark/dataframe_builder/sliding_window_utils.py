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
from copy import deepcopy
from datetime import timedelta
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame as NativeSparkDataFrame, WindowSpec, Column, functions
from pyspark.sql.functions import lag, asc
from pyspark.sql.window import Window

from feathub.processors.spark.dataframe_builder.spark_sql_expr_utils import to_spark_sql_expr

from feathub.common.exceptions import FeathubTransformationException
from feathub.feature_views.sliding_feature_view import (
    SlidingFeatureViewConfig,
    ENABLE_EMPTY_WINDOW_OUTPUT_CONFIG,
    SKIP_SAME_WINDOW_OUTPUT_CONFIG,
)
from feathub.feature_views.transforms.agg_func import AggFunc
from feathub.feature_views.transforms.sliding_window_transform import (
    SlidingWindowTransform,
)
from feathub.processors.constants import EVENT_TIME_ATTRIBUTE_NAME
from feathub.processors.flink.table_builder.aggregation_utils import (
    AggregationFieldDescriptor,
    get_default_value_and_type,
)
from feathub.processors.flink.table_builder.join_utils import join_table_on_key
from feathub.processors.flink.table_builder.time_utils import (
    timedelta_to_flink_sql_interval,
)
from feathub.processors.flink.table_builder.udf import (
    AGG_JAVA_UDF,
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
            to_spark_sql_expr(sliding_window_agg.filter_expr)
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
    window_descriptor: SlidingWindowDescriptor,
    agg_descriptors: List[AggregationFieldDescriptor],
    config: SlidingFeatureViewConfig,
) -> NativeSparkDataFrame:
    descriptor = agg_descriptors[0]

    functions.window("time", "2 days", "1 day")

    window_spec = (
        Window
        # .partitionBy("userId")
        .orderBy(asc("time"))
        .rangeBetween(-1, 0)
    )

    dataframe = dataframe.groupBy(
        functions.window("time", "2 days", "1 day")
    ).sum()

    dataframe.show()

    return dataframe.withColumn(
        descriptor.field_name, functions.expr("sum(cost)")
        .over(window_spec)
        .cast(descriptor.field_data_type)
    )
