package com.uber.stream.ureplicator.worker.offsetmapper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraOffsetMapper implements OffsetMapper{

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraOffsetMapper.class);

    private static final String INSERT_STATEMENT = "INSERT INTO ureplicator.offset_mapping " +
            "(src_topic, src_partition, src_offset, dst_topic, dst_partition, dst_offset)" +
            "VALUES ('%s', %d, %d, '%s', %d, %d);";

    private String cassandraHost;
    private int cassandraPort;
    private CassandraConnector connector;
    private Session session;

    public CassandraOffsetMapper(String cassandraHost, int cassandraPort){
        this.cassandraHost = cassandraHost;
        this.cassandraPort = cassandraPort;
        init();
    }

    private void init(){
        connector = new CassandraConnector();
        connector.connect(cassandraHost, cassandraPort);
        session = connector.getSession();
    }

    static class CassandraConnector {

        private Cluster cluster;

        private Session session;

        public void connect(String node, Integer port) {
            Cluster.Builder b = Cluster.builder().addContactPoint(node);
            if (port != null) {
                b.withPort(port);
            }
            cluster = b.withoutJMXReporting()
                    .build();

            session = cluster.connect();
        }

        public Session getSession() {
            return this.session;
        }

        public void close() {
            session.close();
            cluster.close();
        }
    }

    @Override
    public void mapOffset(String srcTopic, int srcPartition, long srcOffset, String dstTopic, int dstPartition, long dstOffset){
        String insertStmt = String.format(INSERT_STATEMENT, srcTopic, srcPartition, srcOffset, dstTopic, dstPartition, dstOffset);
        ResultSet rs = session.execute(insertStmt);
            LOGGER.info("Finished mapping {}-{}:{} to {}-{}:{}.", srcTopic, srcPartition, srcOffset, dstTopic, dstPartition, dstOffset);
    }

    @Override
    public void close(){
        connector.close();
    }
}
