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
from typing import Sequence

from feathub.processors.flink.table_builder.source_sink_utils_common import get_schema_from_table, \
    generate_random_table_name

from feathub.common.exceptions import FeathubException

from feathub.table.table_descriptor import TableDescriptor
from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    TableResult,
    Schema,
    DataTypes,
)

from feathub.feature_tables.sinks.hbase_sink import HBaseSink, HBaseType


def _get_hbase_connector_name(hbase_type: HBaseType):
    if hbase_type == HBaseType.ALICLOUD:
        return "cloudhbase"
    raise FeathubException(f"Unsuported hbase type {hbase_type}")


def insert_into_hbase_sink(
    t_env: StreamTableEnvironment,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: HBaseSink,
) -> TableResult:
    redis_sink_descriptor_builder = (
        NativeFlinkTableDescriptor.for_connector(_get_hbase_connector_name(sink.hbase_type))
        .schema(get_schema_from_table(features_table))
        .option("table-name", sink.table_name)
        .option("zookeeper.quorum", sink.zookeeper_quorum)
    )

    if sink.flink_extra_config is not None:
        for k, v in sink.flink_extra_config.items():
            redis_sink_descriptor_builder.option(k, v)

    # TODO: Alibaba Cloud Realtime Compute has bug that assumes all the tables should
    # have a name in VVR-6.0.2, which should be fixed in next version VVR-6.0.3. As a
    # current workaround, we have to generate a random table name. We should update the
    # code to use anonymous table sink after VVR-6.0.3 is released.
    random_sink_name = generate_random_table_name("KafkaSink")
    t_env.create_temporary_table(
        random_sink_name, redis_sink_descriptor_builder.build()
    )
    return features_table.execute_insert(random_sink_name)
