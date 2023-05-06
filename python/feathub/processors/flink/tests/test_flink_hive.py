import shutil
import tempfile
import unittest

from feathub.processors.flink.flink_jar_utils import add_jar_to_t_env
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.catalog import HiveCatalog
from testcontainers.core.generic import GenericContainer

from feathub.processors.flink.table_builder.hive_utils import _get_hive_connector_jars

hive_conf_template = """<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://localhost:{hms_port}</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
      <value>...</value>
    </property>

    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
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


class FlinkHiveITTest(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_dir = tempfile.mkdtemp()

        self.hive_container = GenericContainer(image="prestodb/hdp2.6-hive:10")
        self.hive_container.with_exposed_ports(9083)
        self.hive_container.start()

        self.hive_conf_path = tempfile.NamedTemporaryFile(dir=self.temp_dir, suffix=".csv").name
        self.hive_conf_dir = self.temp_dir
        with open(f"{self.hive_conf_dir}/hive-site.xml", "w") as text_file:
            text_file.write(hive_conf_template.format(
                hms_port=self.hive_container.get_exposed_port(9083),
                mysql_url=""))

    def tearDown(self) -> None:
        self.hive_container.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_flink_hive(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        t_env = StreamTableEnvironment.create(env)

        add_jar_to_t_env(t_env, *_get_hive_connector_jars())

        hive_catalog = HiveCatalog("myhive", None, self.hive_conf_dir)
        t_env.register_catalog("myhive", hive_catalog)
