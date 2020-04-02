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
package com.uber.stream.ureplicator.common.observer;

import com.google.common.collect.ImmutableSet;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaBrokerTopicObserver provides topic information on this broker such as all topic names and partitions for each
 * topic
 */
public class KafkaBrokerTopicObserver extends TopicPartitionCountObserver {

  private final static String ZOOKEEPER_TOPIC_OBSERVER_PATH = "/brokers/topics";
  protected final static Set<String> KAFKA_INNER_TOPICS = ImmutableSet.of("__consumer_offsets", "__transaction_state");

  private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerTopicObserver.class);

  public KafkaBrokerTopicObserver(String kakfaClusterName, String clusterRootPath, long refreshTimeIntervalInMillis, ObserverCallback observerCallback) {
    super(kakfaClusterName, clusterRootPath, ZOOKEEPER_TOPIC_OBSERVER_PATH, 30000, 30000,
        refreshTimeIntervalInMillis, observerCallback);
  }

  @Override
  public void updateDataSet() {
    logger.info("updating topic cache map for {} new topics in Kafka cluster {}", topicPartitionMap.size(), kafkaClusterName);
    Set<String> servingTopics;
    servingTopics = new HashSet<>(zkClient.getChildren(topicZkPath));
    servingTopics.removeAll(KAFKA_INNER_TOPICS);

    for (String existedTopic : getAllTopics()) {
      if (!servingTopics.contains(existedTopic)) {
        topicPartitionMap.remove(existedTopic);
      }
    }
    updateTopicPartitionInfoMap(servingTopics);
    logger.info("Now serving {} topics in Kafka cluster: {}", topicPartitionMap.size(), kafkaClusterName);
  }

  @Override
  protected Set<String> pendingRefreshTopics(Set<String> topicsFromZookeeper, Set<String> currentServingTopics) {
    topicsFromZookeeper.removeAll(currentServingTopics);
    return topicsFromZookeeper;
  }
}
