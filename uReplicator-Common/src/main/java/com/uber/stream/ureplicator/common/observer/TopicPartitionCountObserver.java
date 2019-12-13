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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * TopicPartitionCountObserver watches the data change on Kafka ZK to provide the up-to-date topic
 * partition information
 * Usage:
 *  uReplicator Worker use it for 1-1 partition mapping from source to destination cluster.
 */
public class TopicPartitionCountObserver extends PeriodicMonitor implements IZkChildListener {

  private static final Logger logger = LoggerFactory.getLogger(TopicPartitionCountObserver.class);

  private final ZkClient zkClient;
  private final ZkUtils zkUtils;
  private final String clusterRootPath ;
  private final String topicZkPath;
  private final ConcurrentHashMap<String, Integer> topicPartitionMap = new ConcurrentHashMap<String, Integer>();

  public TopicPartitionCountObserver(String clusterRootPath, String topicZkPath,
      int zkConnectionTimeoutMs, int zkSessionTimeoutMs, int refreshIntervalMs) {
    this(new ZkClient(clusterRootPath, zkSessionTimeoutMs, zkConnectionTimeoutMs,
        ZKStringSerializer$.MODULE$), clusterRootPath, topicZkPath, refreshIntervalMs);
  }

  @VisibleForTesting
  protected TopicPartitionCountObserver(ZkClient zkClient, String clusterRootPath, String topicZkPath,
      int refreshIntervalMs) {
    super(refreshIntervalMs, TopicPartitionCountObserver.class.getSimpleName());
    this.zkClient = zkClient;
    this.clusterRootPath = clusterRootPath;
    this.zkUtils = ZkUtils.apply(zkClient, false);
    this.topicZkPath = topicZkPath;
  }

  public void start() {
    logger.info("Starting TopicPartitionCountObserver for zkConnect={} and zkPath={}", clusterRootPath, topicZkPath);
    super.start();
    zkClient.subscribeChildChanges(topicZkPath, this);
    logger.info("TopicPartitionCountObserver started");
  }

  public void shutdown() {
    logger.info("Shutting down TopicPartitionCountObserver for zkConnect={} and zkPath={}", clusterRootPath, topicZkPath);
    super.shutdown();
    zkClient.unsubscribeChildChanges(topicZkPath, this);
    zkUtils.close();
    zkClient.close();
    logger.info("Shutdown TopicPartitionCountObserver finished");
  }

  @Override
  public void updateDataSet() {
    updateTopicPartitionInfoMap(topicPartitionMap.keySet());
  }

  public int getPartitionCount(String topicName) {
    return topicPartitionMap.get(topicName);
  }

  public void addTopic(String topicName) {
    logger.info("Add topic={} to check partition count", topicName);
    topicPartitionMap.putIfAbsent(topicName, 0);
    updateTopicPartitionInfoMap(Collections.singleton(topicName));
  }

  /**
   * deleteTopic() is not safe because msg can be left in memory for consumption even after topic is
   * removed. And the cost to leave unused topics in the map should be negligible since the number
   * of total topics is not large as for now.
   */
  @Override
  public void handleChildChange(String s, List<String> currentChildren) throws Exception {
    final Set<String> topicsFromZookeeper = new HashSet<String>(currentChildren);
    logger.info("Update topics info by zk event for zkPath={}", topicZkPath);
    topicsFromZookeeper.retainAll(topicPartitionMap.keySet());
    updateTopicPartitionInfoMap(topicsFromZookeeper);
  }

  private void updateTopicPartitionInfoMap(final Set<String> topicsToCheck) {
    if (topicsToCheck.size() > 0) {
      // get topic partition count and maybe update partition counts for existing topics
      scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
          zkUtils.getPartitionAssignmentForTopics(
              JavaConversions.asScalaBuffer(ImmutableList.copyOf(topicsToCheck)));

      for (String topic : topicsToCheck) {
        try {
          topicPartitionMap.put(topic, partitionAssignmentForTopics.get(topic).get().size());
        } catch (Exception e) {
          logger.warn("Failed to get topicPartition info for topic={} of zkPath={}",
              topic, topicZkPath, e);
        }
      }
    }
  }
}
