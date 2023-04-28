package org.apache.flink.streaming.connectors.hive.testutil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShimV310;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Objects;

/** Test. */
public class FlinkHiveITTest extends TestLogger {
    @ClassRule public static final TemporaryFolder FOLDER = new TemporaryFolder();
    @ClassRule public static final HiveContainer HIVE_CONTAINER = new HiveContainer();

    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String hiveConfDir = createHiveConfDir().getParent();

        IMetaStoreClient client = new HiveShimV310().getHiveMetastoreClient(createHiveConf());
        System.out.println(client.getAllDatabases());
        Database dataBase =
                new Database(
                        "default",
                        "",
                        "file:/Users/yuri/Documents/projects/feathub/dev-spark/java/data-warehouse",
                        Collections.emptyMap());
        try {
            client.createDatabase(dataBase);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        HiveCatalog catalog = new HiveCatalog("myhive", null, hiveConfDir);

        //        String hiveVersion = null;
        //
        //        new HiveCatalog(
        //                "myhive",
        //                null,
        //                hiveConfDir,
        //                StringUtils.isNullOrWhitespaceOnly(hiveVersion)
        //                        ? HiveShimLoader.getHiveVersion()
        //                        : hiveVersion);

        catalog.open();

        //        CatalogDatabase database = new CatalogDatabaseImpl(Collections.emptyMap(),
        // "null");
        //
        //        catalog.createDatabase("default", database, true);

        tEnv.registerCatalog("myhive", catalog);

        catalog.close();
    }

    private static HiveConf createHiveConf() {
        HiveConf hiveConf = new HiveConf();
        try (InputStream inputStream =
                new FileInputStream(
                        new File(
                                Objects.requireNonNull(
                                                FlinkHiveITTest.class
                                                        .getClassLoader()
                                                        .getResource(HiveCatalog.HIVE_SITE_FILE))
                                        .toURI()))) {
            hiveConf.addResource(inputStream, HiveCatalog.HIVE_SITE_FILE);
            // trigger a read from the conf so that the input stream is read
            hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load hive-site.xml from specified path", e);
        }
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_CONTAINER.getHiveMetastoreURI());
        return hiveConf;
        //        return HiveTestUtils.createHiveConf();
    }

    private static File createHiveConfDir() {
        HiveConf hiveConf = createHiveConf();
        try {
            File site = FOLDER.newFile(HiveCatalog.HIVE_SITE_FILE);
            try (OutputStream out = new FileOutputStream(site)) {
                hiveConf.writeXml(out);
            }
            return site;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create hive conf.", e);
        }
    }
}
