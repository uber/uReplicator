package com.uber.stream.ureplicator.worker.offsetmapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class OffsetMapperFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetMapperFactory.class);

    private static final String OFFSET_MAPPER_TYPE_PROPERTY = "offset.mapper.type";
    private static final String OFFSET_MAPPER_TYPE_CASSANDRA_HOST = "offset.mapper.cassandra.host";
    private static final String OFFSET_MAPPER_TYPE_CASSANDRA_PORT = "offset.mapper.cassandra.port";

    private final Properties props;

    public OffsetMapperFactory(Properties props){
        this.props = props;
    }

    public OffsetMapper getOffsetMapper(){
        String offsetMapperType = props.getProperty(OFFSET_MAPPER_TYPE_PROPERTY);
        switch(offsetMapperType){
            case "cassandra":
                String cassandraHost = props.getProperty(OFFSET_MAPPER_TYPE_CASSANDRA_HOST);
                int cassandraPort = Integer.parseInt(props.getProperty(OFFSET_MAPPER_TYPE_CASSANDRA_PORT));
                LOGGER.info("Using cassandra {}:{} for offset mapping.", cassandraHost, cassandraPort);
                return new CassandraOffsetMapper(cassandraHost, cassandraPort);
            case "kafka":
                LOGGER.info("Using kafka for offset mapping.");
                return new KafkaOffsetMapper(props);
            default:
                LOGGER.info("No option set for offset mapping.");
                return null;
        }
    }
}
