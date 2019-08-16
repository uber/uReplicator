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
package com.uber.streaming.worker.fetcher;


import com.google.common.base.Preconditions;
import com.uber.streaming.worker.Fetcher;
import com.uber.streaming.worker.Sink;
import com.uber.streaming.worker.Task;

import com.uber.streaming.worker.clients.CheckpointManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.GuardedBy;
import kafka.utils.ShutdownableThread;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Consumer Fetcher Thread is the implementation of Fetcher. It fetches ConsumerRecords from
 * Kafka and enqueue to Sink. Fetcher Thread uses Kafka high level consumer.
 */
public class KafkaConsumerFetcherThread extends ShutdownableThread implements
    Fetcher<List<ConsumerRecord>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerFetcherThread.class);

  // updateMapLock guards partitionAddMap and partitionDeleteMap
  private final Object updateMapLock = new Object();
  // partitionMapLock guards partitionMap and kafkaConsumer
  private final Object partitionMapLock = new Object();

  @GuardedBy("partitionMapLock")
  private final Map<TopicPartition, Task> partitionMap = new ConcurrentHashMap<>();
  // partitionAddMap stores the topic partition pending add
  // partitionDeleteMap stores the topic partition pending delete
  // the purpose of using these two fields is for non-blocking add/delete topic partition
  // since partitionMap need to be guard to avoid concurrent topic partition assign on kafkaConsumer
  @GuardedBy("updateMapLock")
  private final Map<TopicPartition, Task> partitionAddMap = new ConcurrentHashMap<>();
  @GuardedBy("updateMapLock")
  private final Map<TopicPartition, Task> partitionDeleteMap = new ConcurrentHashMap<>();
  private final KafkaConsumer kafkaConsumer;

  // last logTopicPartitionInfo time
  private long lastDumpTime = 0L;

  // time interval to log topic partition offset lag
  private final int offsetMonitorMs;

  // The time current thread to wait when message is empty
  private final int fetchBackOffMs;

  // The time spend waiting spent waiting in poll if data is not available in the buffer
  private final int pollTimeoutMs;

  private final CheckpointManager checkpointManager;
  private final KafkaConsumerFetcherConfig config;
  private Sink<List<ConsumerRecord>> processorSink;

  protected KafkaConsumerFetcherThread(String threadName,
      KafkaConsumerFetcherConfig consumerProperties, CheckpointManager checkpointManager) {
    super(threadName, true);
    Preconditions.checkNotNull(threadName, "threadName can't be null");
    Preconditions.checkNotNull(checkpointManager, "checkpointManager can't be null");
    Preconditions.checkNotNull(consumerProperties, "KafkaConsumerFetcherConfig can't be null");
    this.checkpointManager = checkpointManager;
    this.config = consumerProperties;
    this.fetchBackOffMs = config.getFetcherThreadBackoffMs();
    this.offsetMonitorMs = config.getOffsetMonitorInterval();
    this.pollTimeoutMs = config.getPollTimeoutMs();
    this.kafkaConsumer = new KafkaConsumer(consumerProperties);
    // TODO: metrics reporter, rate limiter

  }

  @Override
  public void start() {
    if (processorSink == null) {
      LOGGER.error("data sink required");
      throw new IllegalArgumentException("data sink required");
    }
    super.start();
  }

  @Override
  public void shutdown() {
    if (isShutdownComplete()) {
      return;
    }
    LOGGER.info("[{}] Shutting down fetcher thread", getName());
    initiateShutdown();
    awaitShutdown();
  }

  @Override
  public void awaitShutdown() {
    super.awaitShutdown();
    synchronized (partitionMapLock) {
      partitionMapLock.notifyAll();
      kafkaConsumer.wakeup();
      kafkaConsumer.close();
      partitionMap.clear();
      partitionDeleteMap.clear();
      partitionAddMap.clear();
    }
    LOGGER.info("[{}] Shutdown fetcher thread finished", getName());
  }

  @Override
  public void doWork() {
    synchronized (partitionMapLock) {
      try {
        Map<TopicPartition, Long> seekOffsetTaskMap = new ConcurrentHashMap<>();
        boolean topicChanged = refreshPartitionMap(seekOffsetTaskMap);
        if (topicChanged) {
          LOGGER.info("[{}]Assignment changed, partitionMap {}, partitionResetOffsetMap: {} ",
              getName(),
              partitionMap.keySet(),
              seekOffsetTaskMap);
          kafkaConsumer.assign(partitionMap.keySet());

          if (seekOffsetTaskMap.size() != 0) {
            for (Map.Entry<TopicPartition, Long> entry : seekOffsetTaskMap.entrySet()) {
              long offset = entry.getValue();
              if (offset >= 0) {
                // consumer honors "auto.offset.reset" when offset out of range
                kafkaConsumer.seek(entry.getKey(), offset);
              }
            }
            seekOffsetTaskMap.clear();
          }
        }

        ConsumerRecords records = null;
        if (partitionMap.size() != 0) {
          records = kafkaConsumer.poll(pollTimeoutMs);
        }

        if (partitionMap.size() == 0 || records == null || records.isEmpty()) {
          partitionMapLock.wait(fetchBackOffMs);
          return;
        }
        processFetchedData(records);
      } catch (Throwable e) {
        // TODO: recreate kafka consumer if kafka consumer is closed.
        LOGGER.error("[{}]: Catch Throwable exception", getName(), e);
        // reset offset to fetchOffset to avoid data loss
        for (Map.Entry<TopicPartition, Task> entry : partitionMap.entrySet()) {
          if (checkpointManager.getCheckpointInfo(entry.getValue())
              .getFetchOffset() >= 0) {
            kafkaConsumer.seek(entry.getKey(),
                checkpointManager.getCheckpointInfo(entry.getValue())
                    .getFetchOffset());
          }
        }
      }
    }
    logTopicPartitionOffsetInfo();
  }

  private void processFetchedData(ConsumerRecords consumerRecords) {
    Set<TopicPartition> partitions = consumerRecords.partitions();
    for (TopicPartition tp : partitions) {
      Task task = partitionMap.get(tp);
      if (task == null) {
        LOGGER.warn("Fetched ConsumerRecord for not exists topic partition: {}", tp);
        continue;
      }

      List<ConsumerRecord> records = consumerRecords.records(tp);
      if (records.size() != 0) {
        // TODO: fetch topic partition quota from quota service
        int size = records.size();
        long startOffset = records.get(0).offset();
        long offset = records.get(size - 1).offset();
        if (checkpointManager.getCheckpointInfo(task).bounded(startOffset)) {
          continue;
        }
        processorSink.enqueue(records, task);
        // set next fetch offset after current records enqueue succeed. When fetch request failed, fetcher will reset to fetchOffset to avoid data loss.
        checkpointManager.setFetchOffset(task, offset + 1);
      }
    }
  }

  private void logTopicPartitionOffsetInfo() {
    if ((System.currentTimeMillis() - lastDumpTime) < offsetMonitorMs || partitionMap.size() == 0) {
      return;
    }
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitionMap.keySet());
    if (endOffsets == null) {
      LOGGER.error("[{}]: Failed to find endOffsets for topic partitions: {}", getName(),
          partitionMap.keySet());
      return;
    }
    List<String> topicOffsetPairs = new ArrayList<>();
    for (Map.Entry<TopicPartition, Task> entry : partitionMap.entrySet()) {
      if (endOffsets.containsKey(entry.getKey())) {
        Long lag = endOffsets.get(entry.getKey()) - checkpointManager
            .getCheckpointInfo(entry.getValue()).getFetchOffset();
        topicOffsetPairs
            .add(
                String.format("%s:%d:%d", entry.getKey().toString(),
                    checkpointManager.getCheckpointInfo(entry.getValue()).getFetchOffset(),
                    lag));
      } else {
        LOGGER.warn("[{}]: Failed to find endOffsets for topic partition : {}", getName(),
            entry.getKey());
        continue;
      }
    }
    LOGGER.info("[{}]: Topic partitions dump in fetcher thread: {}", getName(),
        String.join(",", topicOffsetPairs));
    lastDumpTime = System.currentTimeMillis();
  }

  private boolean refreshPartitionMap(Map<TopicPartition, Long> seekOffsetTaskMap) {
    synchronized (updateMapLock) {
      boolean partitionChange = false;
      for (Map.Entry<TopicPartition, Task> addTp : partitionAddMap.entrySet()) {
        TopicPartition tp = addTp.getKey();
        if (!partitionMap.containsKey(tp)) {
          // Avoid duplicate message
          partitionMap.put(tp, addTp.getValue());

          Long startingOffset = checkpointManager.getCheckpointInfo(addTp.getValue(), true)
              .getStartingOffset();
          seekOffsetTaskMap.put(tp, startingOffset);
          partitionChange = true;
        }
      }
      partitionAddMap.clear();

      for (TopicPartition removeTp : partitionDeleteMap.keySet()) {
        if (partitionMap.containsKey(removeTp)) {
          partitionMap.remove(removeTp);
          partitionChange = true;
        }
      }
      partitionDeleteMap.clear();
      return partitionChange;
    }
  }

  private void validateConsumerGroup(Task task) {
    if (StringUtils.isNotBlank(task.getConsumerGroup()) && !task.getConsumerGroup()
        .equals(config.getProperty(ConsumerConfig.GROUP_ID_CONFIG))) {
      throw new RuntimeException(String.format(
          "Consumer group for task is %s doesn't match consumer group for fetcher thread %s",
          task.getConsumerGroup(), config.getProperty(ConsumerConfig.GROUP_ID_CONFIG)));
    }
  }

  @Override
  public void addTask(Task task) {
    LOGGER.trace("[{}]: Enter addTask set {}", getName(),
        task);
    validateConsumerGroup(task);
    synchronized (updateMapLock) {
      // Fetcher thread adds the same topic partition once.
      TopicPartition tp = new TopicPartition(task.getTopic(), task.getPartition());
      if (partitionMap.containsKey(tp)) {
        if (partitionMap.get(tp).equals(task)) {
          LOGGER.warn("[{}]: TopicPartition {} already in current thread", getName(), tp);
        } else {
          LOGGER.error("[{}]: TopicPartition {} already in current thread with a different task",
              getName(), tp);
        }
      }
      partitionAddMap.put(tp, task);
      partitionDeleteMap.remove(tp);
    }
    LOGGER.trace("[{}]: Finish addTask", getName(),
        task);
  }

  @Override
  public void removeTask(Task task) {
    LOGGER.trace("[{}]: Enter removeTask  {}", getName(),
        task);
    validateConsumerGroup(task);
    synchronized (updateMapLock) {
      TopicPartition tp = new TopicPartition(task.getTopic(), task.getPartition());
      if (!partitionMap.containsKey(tp)) {
        return;
      }
      partitionDeleteMap.put(tp, task);
      partitionAddMap.remove(tp);
    }
    LOGGER.trace("[{}]: Finish removeTask {} ", getName(),
        task);
  }

  @Override
  public void pauseTask(Task task) {
    throw new NotImplementedException("pauseTask is not implemented");
  }

  @Override
  public void resumeTask(Task task) {
    throw new NotImplementedException("resumeTask is not implemented");
  }

  @Override
  public List<Task> getTasks() {
    return new ArrayList<>(partitionMap.values());
  }

  @Override
  public void close() {
    shutdown();
  }

  @Override
  public void setNextStage(Sink<List<ConsumerRecord>> sink) {
    processorSink = sink;
  }

  public boolean isRunning() {
    return super.isRunning();
  }


  public static class Builder {

    private String threadName;
    private CheckpointManager checkpointManager;
    private KafkaConsumerFetcherConfig conf;

    public Builder() {

    }

    public Builder setThreadName(String threadName) {
      this.threadName = threadName;
      return this;
    }

    public Builder setCheckpointManager(CheckpointManager checkpointManager) {
      this.checkpointManager = checkpointManager;
      return this;
    }

    public Builder setConf(KafkaConsumerFetcherConfig conf) {
      this.conf = conf;
      return this;
    }

    public KafkaConsumerFetcherThread build() {
      return new KafkaConsumerFetcherThread(threadName, conf, checkpointManager);
    }
  }
}
