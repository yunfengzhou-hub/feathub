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
import hashlib
import os.path
import random
import string
from typing import Optional, Dict, List

from feathub.common.utils import (
    is_local_file_or_dir,
    from_json,
    append_metadata_to_json,
)
from feathub.feature_tables.feature_table import FeatureTable
from feathub.table.schema import Schema

DEFAULT_HIVE_FLINK_PROCESSOR_PROPS = {
    "streaming-source.enable": "true",
    "streaming-source.partition-order": "create-time",
}


def get_hive_catalog_identifier(hive_catalog_conf_dir: str) -> str:
    """
    Return an identifier for the hive catalog configuration. Configurations
    with the same identifier refer to the same Hive catalog.
    """
    if is_local_file_or_dir(hive_catalog_conf_dir):
        hive_catalog_conf_file = os.path.join(hive_catalog_conf_dir, "hive-site.xml")
        hash_md5 = hashlib.md5()
        with open(hive_catalog_conf_file, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    else:
        return "".join(
            random.choice(string.ascii_letters + string.digits) for _ in range(32)
        )


class HiveSource(FeatureTable):
    """
    A source that reads data from Hive.
    """

    def __init__(
        self,
        name: str,
        database: str,
        table: str,
        hive_catalog_conf_dir: str,
        schema: Schema,
        keys: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        timestamp_format: str = "epoch",
        processor_specific_props: Optional[Dict[str, str]] = None,
    ):
        """
        :param name: The name that uniquely identifies this source in a registry.
        :param database: The database to read from.
        :param table: Table name of the table to read from.
        :param schema: The schema of the table.
        :param hive_catalog_conf_dir: URI to your Hive conf dir containing
                                      hive-site.xml. The configuration would be used
                                      to create the Hive Catalog. The URI needs to be
                                      supported by Hadoop FileSystem. If the URI is
                                      relative, i.e. without a scheme, local file
                                      system is assumed.
        :param keys: Optional. The names of fields in this feature view that are
                     necessary to interpret a row of this table. If it is not None, it
                     must be a superset of keys of any feature in this table.
        :param timestamp_field: Optional. If it is not None, it is the name of the field
                                whose values show the time when the corresponding row
                                is generated.
        :param timestamp_format: The format of the timestamp field. See TableDescriptor
                                 for valid format values. Only effective when the
                                 `timestamp_field` is not None.
        :param processor_specific_props: Extra properties to be passthrough to the
                                         processor. The available configurations are
                                         different for different processors.
        """
        super(HiveSource, self).__init__(
            name=name,
            system_name="hive",
            table_uri={
                "table": table,
                "hive_catalog_identifier": get_hive_catalog_identifier(
                    hive_catalog_conf_dir
                ),
                "database": database,
            },
            keys=keys,
            schema=schema,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
        )
        self.name = name
        self.table = table
        self.schema = schema
        self.hive_catalog_conf_dir = hive_catalog_conf_dir
        self.database = database
        self.processor_specific_props = processor_specific_props

    def is_bounded(self) -> bool:
        return False

    @append_metadata_to_json
    def to_json(self) -> Dict:
        return {
            "name": self.name,
            "database": self.database,
            "table": self.table,
            "schema": None if self.schema is None else self.schema.to_json(),
            "keys": self.keys,
            "hive_catalog_conf_dir": self.hive_catalog_conf_dir,
            "timestamp_field": self.timestamp_field,
            "timestamp_format": self.timestamp_format,
            "processor_specific_props": self.processor_specific_props,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> "HiveSource":
        return HiveSource(
            name=json_dict["name"],
            database=json_dict["database"],
            table=json_dict["table"],
            schema=from_json(json_dict["name"])
            if json_dict["name"] is not None
            else None,
            keys=json_dict["keys"],
            hive_catalog_conf_dir=json_dict["hive_catalog_conf_dir"],
            timestamp_field=json_dict["timestamp_field"],
            timestamp_format=json_dict["timestamp_format"],
            processor_specific_props=json_dict["processor_specific_props"],
        )
