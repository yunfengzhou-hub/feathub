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
import random
import string
from typing import Optional

from pyflink.table import (
    StreamTableEnvironment,
    Table as NativeFlinkTable,
    TableResult,
    SqlDialect,
)
from pyflink.table.catalog import HiveCatalog

from feathub.feature_tables.sinks.hive_sink import HiveSink
from feathub.feature_tables.sources.hive_source import HiveSource
from feathub.processors.flink.flink_jar_utils import find_jar_lib, add_jar_to_t_env
from feathub.processors.flink.flink_types_utils import to_sql_type
from feathub.table.table_descriptor import TableDescriptor


def _get_hive_connector_jars() -> list:
    lib_dir = find_jar_lib()
    jar_patterns = [
        "flink-connector-hive_*.jar",
        "hive-exec-*.jar",
        "libfb303-*.jar",
        "antlr-runtime-*.jar",
    ]
    jars = []
    for x in jar_patterns:
        jars.extend(glob.glob(os.path.join(lib_dir, x)))
    return jars


_registered_hive_catalog = dict()
_hive_catalog_name_length = 10


def _register_hive_catalog(
    t_env: StreamTableEnvironment,
    hive_conf_dir: Optional[str],
    database: Optional[str],
) -> str:
    key = (hive_conf_dir, database)
    if key not in _registered_hive_catalog:
        catalog_name = "".join(
            random.choice(string.ascii_letters + string.digits)
            for _ in range(_hive_catalog_name_length)
        )
        catalog = HiveCatalog(catalog_name, database, hive_conf_dir)
        t_env.register_catalog(catalog_name, catalog)
        _registered_hive_catalog[key] = catalog_name
    return _registered_hive_catalog[key]


def get_table_from_hive_source(
    t_env: StreamTableEnvironment,
    source: HiveSource,
) -> NativeFlinkTable:
    add_jar_to_t_env(t_env, *_get_hive_connector_jars())
    hive_catalog_name = _register_hive_catalog(
        t_env, source.hive_conf_dir, source.database
    )
    catalog = t_env.get_current_catalog()
    t_env.use_catalog(hive_catalog_name)

    field_name_type = []
    for field_name, field_type in zip(
        source.schema.field_names, source.schema.field_types
    ):
        field_name_type.append(f"`{field_name}` {to_sql_type(field_type)}")
    schema_sql = ", ".join(field_name_type)

    if source.extra_config:
        property_sql = ", ".join(
            f"'{key}' = '{value}'" for key, value in source.extra_config.items()
        )
        property_sql = f""" TBLPROPERTIES({property_sql})"""
    else:
        property_sql = ""

    create_table_statement = (
        f"CREATE TABLE IF NOT EXISTS {source.table} ({schema_sql}){property_sql};"
    )

    dialect = t_env.get_config().get_sql_dialect()
    t_env.get_config().set_sql_dialect(SqlDialect.HIVE)
    t_env.execute_sql(create_table_statement).wait()
    t_env.get_config().set_sql_dialect(dialect)

    table = t_env.from_path(f"{source.table}")
    t_env.use_catalog(catalog)
    return table


def insert_into_hive_sink(
    t_env: StreamTableEnvironment,
    features_table: NativeFlinkTable,
    features_desc: TableDescriptor,
    sink: HiveSink,
) -> TableResult:
    add_jar_to_t_env(t_env, *_get_hive_connector_jars())
    hive_catalog_name = _register_hive_catalog(t_env, sink.hive_conf_dir, sink.database)
    catalog = t_env.get_current_catalog()
    t_env.use_catalog(hive_catalog_name)

    field_name_type = []
    flink_schema = features_table.get_schema()
    for field_name, field_type in zip(
        flink_schema.get_field_names(), flink_schema.get_field_data_types()
    ):
        field_name_type.append(f"`{field_name}` {to_sql_type(field_type)}")
    schema_sql = ", ".join(field_name_type)

    if sink.extra_config:
        property_sql = ", ".join(
            f"'{key}' = '{value}'" for key, value in sink.extra_config.items()
        )
        property_sql = f""" TBLPROPERTIES({property_sql})"""
    else:
        property_sql = ""

    create_table_statement = (
        f"CREATE TABLE IF NOT EXISTS {features_desc.name} ({schema_sql}){property_sql};"
    )

    dialect = t_env.get_config().get_sql_dialect()
    t_env.get_config().set_sql_dialect(SqlDialect.HIVE)
    t_env.execute_sql(create_table_statement).wait()
    t_env.get_config().set_sql_dialect(dialect)

    result = features_table.execute_insert(features_desc.name)
    t_env.use_catalog(catalog)
    return result
