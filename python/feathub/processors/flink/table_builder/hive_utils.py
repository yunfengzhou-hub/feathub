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
import glob
import os
from typing import Optional, Sequence, cast

from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env

from feathub.table.table_descriptor import TableDescriptor
from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableDescriptor as NativeFlinkTableDescriptor,
    TableResult,
    Schema,
    DataTypes,
)
from pyflink.table.catalog import HiveCatalog

from feathub.feature_tables.sinks.hive_sink import HiveSink
from feathub.feature_tables.sources.hive_source import HiveSource, HiveConfig


def _get_hive_connector_jars() -> list:
    lib_dir = find_jar_lib()
    jar_patterns = [
        "*.jar",
    ]
    jars = []
    for x in jar_patterns:
        jars.extend(glob.glob(os.path.join(lib_dir, x)))
    # for jar in jars:
    #     print(f"hive jar {jar[129:]}")

    return jars


def _register_hive_catalog(t_env: StreamTableEnvironment, config: HiveConfig):
    # check if has been registered

    catalog = HiveCatalog(config.name, config.default_database, config.hive_conf_dir)
    t_env.register_catalog(config.name, catalog)


def get_table_from_hive_source(
    t_env: StreamTableEnvironment,
    source: HiveSource,
) -> NativeFlinkTable:
    add_jar_to_t_env(t_env, *_get_hive_connector_jars())
    _register_hive_catalog(t_env, source.hive_config)
    return t_env.from_path(f"{source.hive_config.name}.{source.name}")


def insert_into_hive_sink(
    t_env: StreamTableEnvironment,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: HiveSink,
) -> TableResult:
    add_jar_to_t_env(t_env, *_get_hive_connector_jars())
    _register_hive_catalog(t_env, sink.hive_config)



    # redis_sink_descriptor_builder = (
    #     NativeFlinkTableDescriptor.for_connector("hive")
    #     .
    # )

    return features_table.execute_insert(f"{sink.hive_config.name}.`default`.{features_desc.name}")
    # t_env.use_catalog(sink.hive_config.name)
    # return features_table.execute_insert(f"{features_desc.name}")
