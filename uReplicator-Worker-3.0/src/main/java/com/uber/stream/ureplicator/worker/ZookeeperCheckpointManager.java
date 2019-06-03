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

import com.uber.stream.ureplicator.worker.interfaces.ICheckPointManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CheckpointManager provides interface allows consumer/producer to access topic partition offset
 * checkpoint. To avoid data loss, commit should execute after producer flush succeed. Offset
 * checkpoint is currently stored on zookeeper.
 */
// TODO: deprecate zookeeper offset checkpoint
public class ZookeeperCheckpointManager implements ICheckPointManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperCheckpointManager.class);
  private final Map<TopicPartition, Long> offsetCheckpoints = new ConcurrentHashMap();
  private final ZkClient commitZkClient;
  private final String groupId;

  public ZookeeperCheckpointManager(CustomizedConsumerConfig config, String groupId) {
    this.commitZkClient = ZkUtils.createZkClient(
        config.getProperty(Constants.COMMIT_ZOOKEEPER_SERVER_CONFIG),
        config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000),
        config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000));
    this.groupId = groupId;
  }

  public void commitOffset(Map<TopicPartition, Long> topicPartitionOffsets) {
    LOGGER.trace("Committing offset to zookeepr for topics: {}", topicPartitionOffsets.keySet());
    for (TopicPartition tp : topicPartitionOffsets.keySet()) {
      commitOffsetToZookeeper(tp, topicPartitionOffsets.get(tp));
    }
  }

  private void commitOffsetToZookeeper(TopicPartition topicPartition, long offset) {
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

  public void shutdown() {
    commitZkClient.close();
  }
}
