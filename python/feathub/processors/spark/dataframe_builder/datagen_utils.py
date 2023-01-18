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
import random
from datetime import datetime
from typing import Callable, Any, List

from pyspark.sql import DataFrame as NativeSparkDataFrame, SparkSession
from pyspark.sql.functions import udf, col

from feathub.common import types
from feathub.common.exceptions import FeathubException
from feathub.feature_tables.sources.datagen_source import DataGenSource, RandomField
from feathub.processors.spark.spark_types_utils import to_spark_type


def _generate_random_field_name(occupied_field_names: List[str]):
    prefix = "seed"
    affix = 0
    while True:
        field_name = prefix + str(affix)
        if field_name in occupied_field_names:
            affix += 1
            continue
        return field_name


def _get_udf_mapping(field_type: types.DType) -> Callable[[int], Any]:
    if field_type == types.Bytes:
        return lambda seed: str(seed).encode()
    elif field_type == types.String:
        return lambda seed: str(seed)
    elif field_type == types.Int64 or field_type == types.Int32:
        return lambda seed: seed
    elif field_type == types.Float64 or field_type == types.Float32:
        return lambda seed: float(seed)
    elif field_type == types.Bool:
        return lambda seed: (seed % 2) == 0
    elif field_type == types.Timestamp:
        return lambda seed: datetime.fromtimestamp(seed)
    elif isinstance(field_type, types.VectorType):
        element_mapping = _get_udf_mapping(field_type.dtype)
        return lambda seed: [element_mapping(seed), ]
    elif isinstance(field_type, types.MapType):
        key_mapping = _get_udf_mapping(field_type.key_dtype)
        value_mapping = _get_udf_mapping(field_type.value_dtype)
        return lambda seed: {key_mapping(seed): value_mapping(seed)}
    else:
        raise FeathubException(f"Unsupported data type {field_type}")


def get_dataframe_from_data_gen_source(
        spark_session: SparkSession, source: DataGenSource
) -> NativeSparkDataFrame:
    if source.number_of_rows is None:
        raise FeathubException()

    seed_field_name = _generate_random_field_name(source.schema.field_names)
    data = spark_session.range(source.number_of_rows).select(col("id").alias(seed_field_name))

    for field_name in source.schema.field_names:
        field_type = source.schema.get_field_type(field_name)
        field_config = source.field_configs.get(field_type)

        mapper = _get_udf_mapping(field_type)

        if isinstance(field_config, RandomField):
            mapper = lambda seed: mapper(random.Random(seed).random())

        mapper_udf = udf(mapper, returnType=to_spark_type(field_type))
        data = data.withColumn(field_name, mapper_udf(seed_field_name))

    data = data.drop(seed_field_name)

    return data
