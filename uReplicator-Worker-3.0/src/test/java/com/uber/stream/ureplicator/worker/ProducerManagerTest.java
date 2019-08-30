package com.uber.stream.ureplicator.worker;

import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProducerManagerTest {

  @Test
  public void testProducerManager() {
    KafkaUReplicatorMetricsReporter.init(null);
    List<ConsumerIterator> iterators = new ArrayList<>();
    BlockingQueue<FetchedDataChunk> channel = new LinkedBlockingQueue<>(5);
    iterators.add(new ConsumerIterator(channel, 500));
    iterators.add(new ConsumerIterator(channel, 500));

    Properties producerProperties = new Properties();
    String testClientId = "testProducerManager";
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    producerProperties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, testClientId);
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");

    ProducerManager producerManager = new ProducerManager(iterators, producerProperties, false,
        null, null, null);
    producerManager.start();
    Assert.assertEquals(producerProperties.getProperty(ProducerConfig.CLIENT_ID_CONFIG),
        testClientId);
  }
}
