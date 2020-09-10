package com.uber.stream.ureplicator.worker.offsetmapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaOffsetMapper implements OffsetMapper{

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetMapper.class);

    private static final String BOOTSTRAP_SERVERS_PROPERTY_NAME = "offset.mapper.kafka.bootstrap.servers";
    private static final String TOPIC_PROPERTY_NAME = "offset.mapper.kafka.topic";
    private static final String CLIENT_ID_PROPERTY_NAME = "offset.mapper.kafka.client.id";
    private static final String KEY_SERIALIZER_PROPERTY_NAME = "offset.mapper.kafka.key.serializer";
    private static final String VALUE_SERIALIZER_PROPERTY_NAME = "offset.mapper.kafka.value.serializer";

    private KafkaProducer<byte[], byte[]> producer;
    private String topic;
    private Properties props;

    public KafkaOffsetMapper(Properties props){
        this.topic = props.getProperty(TOPIC_PROPERTY_NAME);
        this.props = props;
        init();
    }

    private void init(){
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(BOOTSTRAP_SERVERS_PROPERTY_NAME));
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, props.getProperty(CLIENT_ID_PROPERTY_NAME));
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.getProperty(KEY_SERIALIZER_PROPERTY_NAME));
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.getProperty(VALUE_SERIALIZER_PROPERTY_NAME));
        producer = new KafkaProducer<>(producerProps);
    }

    @Override
    public void mapOffset(String srcTopic, int srcPartition, long srcOffset, String dstTopic, int dstPartition, long dstOffset){
        OffsetMapping mapping = new OffsetMapping(srcTopic, dstTopic, srcPartition, dstPartition, srcOffset, dstOffset);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, mapping.toString().getBytes(), mapping.toString().getBytes());
        producer.send(record);
        LOGGER.info("Finished mapping {}-{}:{} to {}-{}:{}.", srcTopic, srcPartition, srcOffset, dstTopic, dstPartition, dstOffset);
    }

    @Override
    public void close(){
        producer.close();
    }
}
