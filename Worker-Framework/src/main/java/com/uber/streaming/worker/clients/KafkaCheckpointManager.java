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
package com.uber.streaming.worker.clients;

import com.uber.streaming.worker.Task;
import com.uber.streaming.worker.fetcher.KafkaConsumerFetcherConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.GuardedBy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaCheckpointManager uses kafka consumer group offset for topic partition offset management
 */
public class KafkaCheckpointManager extends AbstractCheckpointManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCheckpointManager.class);

  @GuardedBy("kafkaConsumersLock")
  private final Map<String, KafkaConsumer> kafkaConsumers;
  private final Properties consumerProps;
  // kafkaConsumerCreationLock guards new KafkaConsumer creation
  private final Object kafkaConsumerCreationLock = new Object();

  // kafkaConsumerLock guards access on KafkaConsumer by lock whole kafkaConsumers
  // It's acceptable for now since access on KafkaConsumer will only trigger periodically
  // TODO:(yayang) create ThreadsafeKafkaConsumer for thread safe access on KafkaConsumer
  private final Object kafkaConsumerLock = new Object();

  private final Map<Task, CheckpointInfo> checkpointInfoMap;

  private final String defaultConsumerGroup;

  public KafkaCheckpointManager(Properties consumerProps) {
    this.defaultConsumerGroup = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    this.kafkaConsumers = new ConcurrentHashMap<>();
    this.checkpointInfoMap = new ConcurrentHashMap<>();
    this.consumerProps = consumerProps;
  }

  @Override
  public void commitOffset() {
    commitOffset(new ArrayList<>(checkpointInfoMap.keySet()));
  }

  @Override
  public void commitOffset(List<Task> tasks) {
    // TODO(yayang): implement batch commit, add metrics for commit rate, failure rate
    int count = 0;
    for (Task task : tasks) {
      if (commitOffset(task)) {
        count++;
      }
    }
    LOGGER.info("commit offset finished, num of tasks : {}", count);
  }

  @Override
  public CheckpointInfo getCheckpointInfo(Task task) {
    return getCheckpointInfo(task, false);
  }

  // commit in-memory offset info to __consumer_offsets
  private boolean commitOffset(Task task) {
    TopicPartition topicPartition = new TopicPartition(task.getTopic(), task.getPartition());

    try {
      CheckpointInfo checkpointInfo = checkpointInfoMap.get(task);

      if (checkpointInfo == null) {
        LOGGER.error(
            "skip committing offset for topic partition {} because of checkpoint info not found.",
            topicPartition);
      } else if (checkpointInfo.getCommitOffset() == -1) {
        LOGGER.debug(
            "skip committing offset for topic partition {} because of checkpoint info is -1.",
            topicPartition);
      } else {
        synchronized (kafkaConsumerLock) {
          KafkaConsumer kafkaConsumer = getKafkaConsumer(task, true);
          if (kafkaConsumer == null) {
            throw new RuntimeException(
                String.format("kafkaConsumer not found for topic partition %s",
                    topicPartition)
            );
          }
          OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(
              checkpointInfo.getCommitOffset());
          kafkaConsumer.commitSync(Collections.singletonMap(topicPartition, offsetAndMetadata));
        }
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("commitOffset for topic partition {} failed", topicPartition, e);
    }
    return false;
  }

  private KafkaConsumer getKafkaConsumer(Task task, boolean createOnEmpty) {
    String consumerGroup = task.getConsumerGroup(defaultConsumerGroup);

    if (createOnEmpty && !kafkaConsumers.containsKey(consumerGroup)) {
      synchronized (kafkaConsumerCreationLock) {
        if (kafkaConsumers.containsKey(consumerGroup)) {
          return kafkaConsumers.get(consumerGroup);
        }
        KafkaConsumerFetcherConfig consumerFetcherConfig = new KafkaConsumerFetcherConfig(
            consumerProps);
        consumerFetcherConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        KafkaConsumer kafkaConsumer = new KafkaConsumer(consumerFetcherConfig);
        kafkaConsumers.putIfAbsent(consumerGroup, kafkaConsumer);
      }
    }
    return kafkaConsumers.get(consumerGroup);
  }

  @Override
  public CheckpointInfo getCheckpointInfo(Task task, boolean refresh) {
    if (!checkpointInfoMap.containsKey(task) || refresh) {
      CheckpointInfo checkpointInfo = new CheckpointInfo(task,
          task.getStartOffset() != null ? task.getStartOffset() : -1,
          task.getEndOffset());
      checkpointInfoMap.put(task, checkpointInfo);
    }
    return checkpointInfoMap.get(task);
  }

  @Override
  public void close() {
    for (KafkaConsumer consumer : kafkaConsumers.values()) {
      consumer.wakeup();
      consumer.close();
    }
    checkpointInfoMap.clear();
  }
}
