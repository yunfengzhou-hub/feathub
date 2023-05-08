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

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableResult,
)

from feathub.common.exceptions import FeathubException
from feathub.feature_tables.feature_table import FeatureTable
from feathub.feature_tables.sinks.black_hole_sink import BlackHoleSink
from feathub.feature_tables.sinks.file_system_sink import FileSystemSink
from feathub.feature_tables.sinks.hive_sink import HiveSink
from feathub.feature_tables.sinks.kafka_sink import KafkaSink
from feathub.feature_tables.sinks.mysql_sink import MySQLSink
from feathub.feature_tables.sinks.print_sink import PrintSink
from feathub.feature_tables.sinks.redis_sink import RedisSink
from feathub.feature_tables.sources.datagen_source import DataGenSource
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_tables.sources.hive_source import HiveSource
from feathub.feature_tables.sources.kafka_source import KafkaSource
from feathub.processors.flink.table_builder.black_hole_utils import (
    insert_into_black_hole_sink,
)
from feathub.processors.flink.table_builder.datagen_utils import (
    get_table_from_data_gen_source,
)
from feathub.processors.flink.table_builder.file_system_utils import (
    get_table_from_file_source,
    insert_into_file_sink,
)
from feathub.processors.flink.table_builder.hive_utils import (
    get_table_from_hive_source,
    insert_into_hive_sink,
)
from feathub.processors.flink.table_builder.kafka_utils import (
    get_table_from_kafka_source,
    insert_into_kafka_sink,
)
from feathub.processors.flink.table_builder.mysql_utils import (
    insert_into_mysql_sink,
)
from feathub.processors.flink.table_builder.print_utils import insert_into_print_sink
from feathub.processors.flink.table_builder.redis_utils import insert_into_redis_sink
from feathub.table.table_descriptor import TableDescriptor


def get_table_from_source(
    t_env: StreamTableEnvironment, source: FeatureTable
) -> NativeFlinkTable:
    """
    Get the Flink Table from the given source.

    :param t_env: The StreamTableEnvironment under which the source table will be
                  created.
    :param source: The source.
    :return: The flink table.
    """
    if isinstance(source, FileSystemSource):
        return get_table_from_file_source(t_env, source)
    elif isinstance(source, KafkaSource):
        return get_table_from_kafka_source(t_env, source, source.keys)
    elif isinstance(source, HiveSource):
        return get_table_from_hive_source(t_env, source)
    elif isinstance(source, DataGenSource):
        return get_table_from_data_gen_source(t_env, source)
    else:
        raise FeathubException(f"Unsupported source type {type(source)}.")


def insert_into_sink(
    t_env: StreamTableEnvironment,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: FeatureTable,
) -> TableResult:
    """
    Insert the flink table to the given sink.
    """
    if isinstance(sink, FileSystemSink):
        return insert_into_file_sink(t_env, features_table, sink)
    elif isinstance(sink, KafkaSink):
        return insert_into_kafka_sink(t_env, features_table, sink, features_desc.keys)
    elif isinstance(sink, PrintSink):
        return insert_into_print_sink(features_table)
    elif isinstance(sink, RedisSink):
        return insert_into_redis_sink(t_env, features_table, features_desc, sink)
    elif isinstance(sink, HiveSink):
        return insert_into_hive_sink(t_env, features_table, features_desc, sink)
    elif isinstance(sink, MySQLSink):
        return insert_into_mysql_sink(t_env, features_table, sink, features_desc.keys)
    elif isinstance(sink, BlackHoleSink):
        return insert_into_black_hole_sink(features_table)
    else:
        raise FeathubException(f"Unsupported sink type {type(sink)}.")
