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
import os
import tempfile
from abc import ABC

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from testcontainers.core.generic import GenericContainer

from feathub.processors.flink.flink_jar_utils import add_jar_to_t_env
from pyflink.java_gateway import get_gateway

from feathub.feature_tables.sinks.hive_sink import HiveSink
from feathub.feature_tables.sources.hive_source import HiveConfig
from feathub.processors.flink.table_builder.hive_utils import _get_hive_connector_jars
from feathub.tests.feathub_it_test_base import FeathubITTestBase

hive_conf_template = """<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hive.metastore.uris</name>
<!--        <value>thrift://127.0.0.1:9083</value>-->
        <value>thrift://localhost:{hms_port}</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionURL</name>

<!--        <value>jdbc:{mysql_url}?useSSL=false</value>-->
        <value>jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
<!--        <value>com.mysql.cj.jdbc.Driver</value>-->
      <value>com.mysql.jdbc.Driver</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
<!--        <value>root</value>-->
      <value>...</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
<!--        <value>root</value>-->
      <value>...</value>
    </property>

    <property>
        <name>hive.metastore.connect.retries</name>
        <value>15</value>
    </property>

    <property>
        <name>hive.metastore.disallow.incompatible.col.type.changes</name>
        <value>false</value>
    </property>

    <property>
        <!-- https://community.hortonworks.com/content/supportkb/247055/errorjavalangunsupportedoperationexception-storage.html -->
        <name>metastore.storage.schema.reader.impl</name>
        <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
    </property>

    <property>
        <name>hive.support.concurrency</name>
        <value>true</value>
    </property>

    <property>
        <name>hive.txn.manager</name>
        <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
    </property>

    <property>
        <name>hive.compactor.initiator.on</name>
        <value>true</value>
    </property>

    <property>
        <name>hive.compactor.worker.threads</name>
        <value>1</value>
    </property>

    <property>
        <name>hive.users.in.admin.role</name>
        <value>hdfs,hive</value>
    </property>

</configuration>

"""


class HiveSourceSinkITTest(ABC, FeathubITTestBase):
    def setUp(self) -> None:
        FeathubITTestBase.setUp(self)
        # print(self.hive_conf_dir + str(os.listdir(self.hive_conf_dir)))
        # hive_conf = f"""
        #
        # """
        # env = StreamExecutionEnvironment.get_execution_environment()
        # t_env = StreamTableEnvironment.create(env)
        # add_jar_to_t_env(t_env, *_get_hive_connector_jars())
        self.hive_container = GenericContainer(image="prestodb/hdp2.6-hive:10")
        self.hive_container.with_exposed_ports(9083)
        # self.hive_container.with_bind_ports(9083, 9083)
        # self.hive_container = get_gateway().jvm.org.apache.flink.streaming.connectors.hive.testutil.HiveContainer()
        self.hive_container.start()

        # self.setup_mysql_container()

        self.hive_conf_path = tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".csv").name
        self.hive_conf_dir = self.temp_dir
        # print(self.mysql_container.get_connection_url())
        # print(self.mysql_container.get_exposed_port(self.mysql_container.port_to_expose))
        with open(f"{self.hive_conf_dir}/hive-site.xml", "w") as text_file:
            text_file.write(hive_conf_template.format(
                hms_port=self.hive_container.get_exposed_port(9083),
                mysql_url=""))
            # text_file.write(hive_conf_template.format(
            #     mysql_url=self.mysql_container.get_connection_url()))

        # self.hive_catalog = get_gateway().jvm.org.apache.flink.table.catalog.hive.HiveTestUtils.createHiveCatalog()
        # print(self.hive_catalog.toString())
        # self.hive_catalog.open()

    def tearDown(self) -> None:
        FeathubITTestBase.tearDown(self)
        self.hive_container.stop()
        self.teardown_mysql_container()
        # self.hive_catalog.close()

    def test_hive_sink(self):
        source = self.create_file_source(self.input_data.copy())
        # print(self.hive_conf_dir + str(os.listdir(self.hive_conf_dir)))

        hive_config = HiveConfig(
            name="test_hive_config",
            # hive_conf_dir="/Users/yuri/Documents/projects/feathub/dev-spark/python/feathub/feature_tables/tests",
            hive_conf_dir=self.hive_conf_dir,
            default_database=None,
            hadoop_conf_dir=None,
        )

        sink = HiveSink(hive_config)

        self.client.materialize_features(
            features=source,
            sink=sink,
            allow_overwrite=True,
        ).wait()
