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
from typing import Optional, Sequence

from pyflink.table import (
    StreamTableEnvironment,
    Schema as NativeFlinkSchema,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
)

from feathub.feature_tables.sources.hbase_source import HBaseSource
from feathub.processors.flink.flink_types_utils import to_flink_schema
from feathub.processors.flink.table_builder.source_sink_utils_common import (
    define_watermark,
    generate_random_table_name,
)
from feathub.table.schema import Schema


def _get_flink_schema(schema: Schema, column_families: Optional[Sequence[Sequence[str]]]) -> NativeFlinkSchema:
    if column_families is None:
        return to_flink_schema(schema)




def get_table_from_data_gen_source(
    t_env: StreamTableEnvironment, hbase_source: HBaseSource
) -> NativeFlinkTable:
    flink_schema = to_flink_schema(hbase_source.schema)

    flink_schema.

    # Define watermark if the kafka_source has timestamp field
    if hbase_source.timestamp_field is not None:
        flink_schema = define_watermark(
            t_env,
            flink_schema,
            hbase_source.max_out_of_orderness,
            hbase_source.timestamp_field,
            hbase_source.timestamp_format,
            hbase_source.schema.get_field_type(hbase_source.timestamp_field),
        )

    table_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector(
            "hbase-2.2"
        ).schema(flink_schema)
        .option("table-name", hbase_source.table_name)
        .option("zookeeper.quorum", hbase_source.zk_quorum)
    )

    table_name = generate_random_table_name(hbase_source.name)
    t_env.create_temporary_table(table_name, table_descriptor_builder.build())
    return t_env.from_path(table_name)

