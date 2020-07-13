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
package com.uber.stream.ureplicator.worker;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.worker.interfaces.ICheckPointManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CheckpointManager provides interface allows consumer/producer to access topic partition offset checkpoint. To avoid
 * data loss, commit should execute after producer flush succeed. Offset checkpoint is currently stored on zookeeper.
 */
// TODO: deprecate zookeeper offset checkpoint
public class ZookeeperCheckpointManager implements ICheckPointManager {

  private final ExecutorService commitExecutor;

  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperCheckpointManager.class);
  private final Map<TopicPartition, Long> offsetCheckpoints = new ConcurrentHashMap();
  private final ZkClient commitZkClient;
  private final String groupId;
  protected Meter commitFailure = new Meter();
  private static final String COMMIT_FAILURE_METER_NAME = "commitFailure";

  public ZookeeperCheckpointManager(CustomizedConsumerConfig config, String groupId) {
    this(ZkUtils.createZkClient(
        config.getProperty(Constants.COMMIT_ZOOKEEPER_SERVER_CONFIG),
        config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000),
        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000)), groupId);
  }

  @VisibleForTesting
  protected ZookeeperCheckpointManager(ZkClient commitZkClient, String groupId) {
    this.commitZkClient = commitZkClient;
    this.groupId = groupId;
    this.commitExecutor = Executors.newFixedThreadPool(10);
    KafkaUReplicatorMetricsReporter.get().registerMetric(COMMIT_FAILURE_METER_NAME, commitFailure);
  }

  public boolean commitOffset(Map<TopicPartition, Long> topicPartitionOffsets) {
    if (topicPartitionOffsets == null || topicPartitionOffsets.size() == 0) {
      return true;
    }
    LOGGER.trace("Committing offset to zookeepr for topics: {}", topicPartitionOffsets.keySet());
    List<CompletableFuture<Void>> futureList = new ArrayList<>();
    for (Map.Entry<TopicPartition, Long> entry : topicPartitionOffsets.entrySet()) {
      CompletableFuture<Void> future = CompletableFuture
          .runAsync(() -> commitOffsetToZookeeper(entry.getKey(), entry.getValue()),
              commitExecutor);
      futureList.add(future);
    }
    try {
      CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()])).get();
    } catch (InterruptedException e) {
      LOGGER.error("Caught InterruptedException on commitOffset.", e);
      commitFailure.mark(topicPartitionOffsets.size());
      return false;
    } catch (ExecutionException e) {
      LOGGER.error("Caught ExecutionException on commitOffset.", e);
      commitFailure.mark(topicPartitionOffsets.size());
      return false;
    }
    LOGGER.info("commitOffset finished, number of topics: {}", topicPartitionOffsets.size());
    return true;
  }

  private void commitOffsetToZookeeper(TopicPartition topicPartition, long offset) {
    try {
      if (!offsetCheckpoints.containsKey(topicPartition)
          || offsetCheckpoints.get(topicPartition) != offset) {
        ZKGroupTopicDirs dirs = new ZKGroupTopicDirs(groupId, topicPartition.topic());
        String path = dirs.consumerOffsetDir() + "/" + topicPartition.partition();
        if (!commitZkClient.exists(path)) {
          commitZkClient.createPersistent(path, true);
        }
        commitZkClient.writeData(path,
            String.valueOf(offset));
        offsetCheckpoints.put(topicPartition, offset);
      }
    } catch (Exception e) {
      LOGGER.error("Caught Exception on commitOffset for topic: {}, partition: {}, offset: {}", topicPartition.topic(),
          topicPartition.partition(), offset, e);
      throw e;
    }
  }

  public Long fetchOffset(TopicPartition topicPartition) {
    ZKGroupTopicDirs dirs = new ZKGroupTopicDirs(groupId, topicPartition.topic());
    String path = dirs.consumerOffsetDir() + "/" + topicPartition.partition();
    if (!commitZkClient.exists(path)) {
      return -1L;
    }
    String offset = commitZkClient.readData(path).toString();
    if (StringUtils.isEmpty(offset)) {
      return -1L;
    }
    try {
      return Long.parseLong(offset);
    } catch (Exception e) {
      LOGGER.warn("Parse offset {} for topic partition failed, zk path: {}", offset, path);
      return -1L;
    }
  }

  @Override
  public void shutdown() {
    commitZkClient.close();
    KafkaUReplicatorMetricsReporter.get().removeMetric(COMMIT_FAILURE_METER_NAME);
    commitExecutor.shutdown();
    try {
      if (!commitExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
        LOGGER.error("Shutdown commitExecutor timeout");
      }
    } catch (InterruptedException e) {
      LOGGER.error("Shutdown commitExecutor failed", e);
    }
    offsetCheckpoints.clear();
  }
}
