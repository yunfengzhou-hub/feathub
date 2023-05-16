# Copyright 2022 The FeatHub Authors
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

from testcontainers.mysql import MySqlContainer

from feathub.common import types
from feathub.feathub_client import FeathubClient
from feathub.feature_tables.sources.file_system_source import FileSystemSource
from feathub.feature_views.derived_feature_view import DerivedFeatureView
from feathub.feature_views.feature import Feature
from feathub.registries.tests.test_registry import RegistryTestBase
from feathub.table.schema import Schema


class MySqlRegistryTest(RegistryTestBase):
    __test__ = True

    mysql_container = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.mysql_container = MySqlContainer(image="mysql:8.0")
        cls.mysql_container.start()
        cls.client = FeathubClient(
            {
                "processor": {
                    "type": "local",
                },
                "online_store": {
                    "types": ["memory"],
                    "memory": {},
                },
                "registry": {
                    "type": "mysql",
                    "mysql": {
                        "database": cls.mysql_container.MYSQL_DATABASE,
                        "table": "feathub_registry_features",
                        "host": cls.mysql_container.get_container_host_ip(),
                        "port": int(
                            cls.mysql_container.get_exposed_port(
                                cls.mysql_container.port_to_expose
                            )
                        ),
                        "username": cls.mysql_container.MYSQL_USER,
                        "password": cls.mysql_container.MYSQL_PASSWORD,
                    },
                },
                "feature_service": {
                    "type": "local",
                    "local": {},
                },
            }
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.mysql_container.stop()
        cls.mysql_container = None

    def tearDown(self) -> None:
        super().tearDown()
        cursor = self.client.registry.conn.cursor()
        cursor.execute("DELETE FROM feathub_registry_features;")
        cursor.close()

    def _get_client(self) -> FeathubClient:
        return self.client

    def test_resolved_feature(self):
        source, features = self._create_feature()
        self.client.registry.register_features(source)
        features = self.client.registry.build_features([features])[0]
        loaded_features = self.client.registry.get_features("feature_view")
        self.assertEquals(features.to_json(), loaded_features.to_json())

    def test_unresolved_feature(self):
        source, features = self._create_feature()
        self.client.registry.register_features(features)
        loaded_features = self.client.registry.get_features("feature_view")
        self.assertEquals(features.to_json(), loaded_features.to_json())

    @staticmethod
    def _create_feature():
        schema = (
            Schema.new_builder()
            .column("name", types.String)
            .column("cost", types.Int64)
            .column("distance", types.Int64)
            .column("time", types.String)
            .build()
        )

        source = FileSystemSource(
            name="source_name",
            path="source_path",
            data_format="csv",
            schema=schema,
            keys=["name"],
        )

        features = DerivedFeatureView(
            name="feature_view",
            source="source_name",
            features=[
                Feature(
                    name="name_without_alex",
                    dtype=types.String,
                    transform="name",
                    keys=["name"],
                )
            ],
            keep_source_fields=True,
            filter_expr="name_without_alex IS NOT NULL",
        )

        return source, features
