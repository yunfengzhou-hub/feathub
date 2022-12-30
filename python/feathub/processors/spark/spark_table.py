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
from collections import defaultdict
from datetime import timedelta, datetime
from typing import Optional, Union

import pandas as pd
from pyspark.sql import DataFrame as NativeSparkDataFrame

from feathub.common.exceptions import FeathubException
from feathub.common.types import to_numpy_dtype
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.processors.spark.spark_job import SparkJob
from feathub.processors.spark.spark_types_utils import (
    to_feathub_schema,
    to_feathub_type,
)
from feathub.table.schema import Schema
from feathub.table.table import Table
from feathub.table.table_descriptor import TableDescriptor

if typing.TYPE_CHECKING:
    from feathub.processors.spark.spark_processor import SparkProcessor


class SparkTable(Table):
    def __init__(
        self,
        spark_processor: "SparkProcessor",
        feature: TableDescriptor,
        keys: Union[pd.DataFrame, TableDescriptor, None],
        start_datetime: Optional[datetime],
        end_datetime: Optional[datetime],
    ) -> None:
        super().__init__(feature.timestamp_field, feature.timestamp_format)
        self.spark_processor = spark_processor
        self.feature = feature
        self.keys = keys
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime

        if self.keys is not None:
            # TODO: Add support for keys after Spark processor supports join transform.
            raise FeathubException(
                "Spark processor does not support features with keys."
            )

        if self.start_datetime is not None or self.end_datetime is not None:
            # TODO: Add support for timestamp and watermark with window transform.
            raise FeathubException(
                "Spark processor does not support filtering features with "
                "start/end datetime."
            )

    def get_schema(self) -> Schema:
        return to_feathub_schema(self._get_spark_dataframe(self.feature).schema)

    def to_pandas(self, force_bounded: bool = False) -> pd.DataFrame:
        feature = self.feature
        if not feature.is_bounded():
            if not force_bounded:
                raise FeathubException(
                    "Unbounded table cannot be converted to Pandas DataFrame. You can "
                    "set force_bounded to True to convert the Table to DataFrame."
                )
            feature = feature.get_bounded_view()

        dataframe = self._get_spark_dataframe(feature)

        field_names = []
        field_types = []
        for field in dataframe.schema.fields:
            field_names.append(field.name)
            field_types.append(to_numpy_dtype(to_feathub_type(field.dataType)))

        results = dataframe.collect()

        data: typing.Dict[str, typing.List[typing.Any]] = defaultdict(list)
        for row in results:
            for name, value in zip(field_names, row):
                data[name].append(value)

        values = [value for _, value in data.items()]
        return pd.DataFrame(
            {
                field_names[i]: pd.Series(values[i], dtype=field_types[i])
                for i in range(len(field_names))
            }
        )

    def execute_insert(
        self,
        sink: FeatureTable,
        ttl: Optional[timedelta] = None,
        allow_overwrite: bool = False,
    ) -> SparkJob:
        dataframe = self._get_spark_dataframe(self.feature)

        if isinstance(sink, FileSystemSink):
            future = self.spark_processor.executor.submit(
                dataframe.write.format(sink.data_format).save, path=sink.path
            )
        elif isinstance(sink, PrintSink):
            future = self.spark_processor.executor.submit(dataframe.show)
        else:
            raise FeathubException(f"Unsupported sink {type(sink)}.")

        return SparkJob(future, self.spark_processor.executor)

    def _get_spark_dataframe(self, feature: TableDescriptor) -> NativeSparkDataFrame:
        return self.spark_processor.dataframe_builder.build(feature)
