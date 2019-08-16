/*
 * Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uber.streaming.worker;

import com.uber.streaming.worker.fetcher.KafkaConsumerFetcherConfig;
import com.uber.streaming.worker.utils.KafkaStarterUtils;
import com.uber.streaming.worker.utils.ZkStarter;
import java.util.List;
import java.util.Properties;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  private static KafkaProducer createProducer(String bootstrapServer) {
    Properties producerProps = new Properties();
    producerProps
        .setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    KafkaProducer producer = new KafkaProducer(producerProps);
    return producer;
  }

  public static void produceMessages(String bootstrapServer, String topicname, int messageCount) {
    KafkaProducer producer = createProducer(bootstrapServer);
    for (int i = 0; i < messageCount; i++) {
      ProducerRecord<Byte[], Byte[]> record = new ProducerRecord(topicname, null,
          String.format("Test Value - %d", i).getBytes());
      producer.send(record);
    }
    producer.flush();
    producer.close();
  }

  public static void produceMessages(String bootstrapServer, String topicname, int messageCount,
      int numOfPartitions) {
    KafkaProducer producer = createProducer(bootstrapServer);
    for (int i = 0; i < messageCount; i++) {
      ProducerRecord<Byte[], Byte[]> record = new ProducerRecord(topicname, i % numOfPartitions,
          null,
          String.format("Test Value - %d", i).getBytes());
      producer.send(record);
    }
    producer.flush();
    producer.close();
  }

  public static Properties createKafkaConsumerProperties(String bootstrap) {
    return createKafkaConsumerProperties(bootstrap, "earliest");
  }

  public static Properties createKafkaConsumerProperties(String bootstrap, String autoOffsetReset) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
        "uReplicator");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    properties.setProperty(KafkaConsumerFetcherConfig.POLL_TIMEOUT_MS, "10");
    properties.setProperty(KafkaConsumerFetcherConfig.FETCHER_THREAD_BACKOFF_MS, "10");
    properties.setProperty("refresh.backoff.ms", "50");
    properties.setProperty("consumer.timeout.ms", "50");
    return properties;
  }

  public static KafkaServerStartable startupKafka(String clusterZk, int clusterPort,
      List<String> topics) {
    // start zk for unittest
    ZkStarter.startLocalZkServer();
    // start kafka for unittest
    KafkaServerStartable kafka = KafkaStarterUtils
        .startServer(clusterPort, KafkaStarterUtils.DEFAULT_BROKER_ID, clusterZk,
            KafkaStarterUtils.getDefaultKafkaConfiguration());
    kafka.startup();
    for (String topic : topics) {
      KafkaStarterUtils.createTopic(topic, clusterZk);
    }
    return kafka;
  }
}
