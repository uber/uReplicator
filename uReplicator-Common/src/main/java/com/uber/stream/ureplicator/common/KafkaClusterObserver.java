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
package com.uber.stream.ureplicator.common;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// TODO: implement topic count observer
public class KafkaClusterObserver implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterObserver.class);

  private static final String DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

  private final KafkaConsumer kafkaConsumer;

  public KafkaClusterObserver(String bootstrapServer) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        DESERIALIZER_CLASS);
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        DESERIALIZER_CLASS);
    this.kafkaConsumer = new KafkaConsumer(properties);
  }

  @VisibleForTesting
  protected KafkaClusterObserver(KafkaConsumer kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
  }

  public Map<TopicPartition, Integer> findLeaderForPartitions(
      List<TopicPartition> topicPartitions) {
    Map<TopicPartition, Integer> partitionsWithLeader = new HashMap<>();
    if (topicPartitions.size() == 0) {
      return partitionsWithLeader;
    }
    Map<String, List<PartitionInfo>> topicInfo = kafkaConsumer.listTopics();
    if (topicInfo == null) {
      return partitionsWithLeader;
    }

    for (TopicPartition tp : topicPartitions) {
      if (topicInfo.containsKey(tp.topic())) {
        LOGGER.error("unable to find topic {} in cluster observer", tp.topic());
      }
      Integer leader = findLeaderId(tp, topicInfo.get(tp.topic()));
      if (leader != null) {
        partitionsWithLeader.put(tp, leader);
      }
    }
    return partitionsWithLeader;
  }

  private Integer findLeaderId(TopicPartition topicPartition,
      List<PartitionInfo> partitionInfoList) {
    if (partitionInfoList == null) {
      return null;
    }
    int numOfPartition = partitionInfoList.size();
    Optional<PartitionInfo> optInfo = partitionInfoList.stream()
        .filter(x -> x.partition() == topicPartition.partition() % numOfPartition).findFirst();

    if (optInfo.isPresent() && optInfo.get().leader() != null) {
      return optInfo.get().leader().id();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    kafkaConsumer.close();
  }
}
