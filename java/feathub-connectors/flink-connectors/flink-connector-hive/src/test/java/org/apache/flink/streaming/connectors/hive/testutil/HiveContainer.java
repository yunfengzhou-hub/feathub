package org.apache.flink.streaming.connectors.hive.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

/** Test Container for hive. */
public class HiveContainer extends GenericContainer<HiveContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveContainer.class);

    public static final String HOST_NAME = "hadoop-master";
    public static final int HIVE_METASTORE_PORT = 9083;

    public HiveContainer() {
        super(DockerImageName.parse("prestodb/hdp2.6-hive:10"));
        withExtraHost(HOST_NAME, "127.0.0.1");
        addExposedPort(HIVE_METASTORE_PORT);
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (LOG.isInfoEnabled()) {
            followOutput(new Slf4jLogConsumer(LOG));
        }
    }

    public String getHiveMetastoreURI() {
        return String.format("thrift://%s:%s", getHost(), getMappedPort(HIVE_METASTORE_PORT));
    }
}
