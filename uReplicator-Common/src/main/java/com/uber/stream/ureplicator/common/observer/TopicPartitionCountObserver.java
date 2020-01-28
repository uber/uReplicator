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

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * TopicPartitionCountObserver watches the data change on Kafka ZK to provide the up-to-date topic partition information
 * Usage: uReplicator Worker uses it for 1-1 partition mapping from source to destination cluster.
 */
public class TopicPartitionCountObserver extends PeriodicMonitor implements IZkChildListener, IZkDataListener {

  private static final Logger logger = LoggerFactory.getLogger(TopicPartitionCountObserver.class);
  private final static String METRIC_TEMPLATE = "KafkaBrokerTopicObserver.%s.%s";

  protected final ZkClient zkClient;
  protected final ZkUtils zkUtils;
  protected final String clusterRootPath;
  protected final String topicZkPath;
  protected final ConcurrentHashMap<String, Integer> topicPartitionMap = new ConcurrentHashMap<String, Integer>();
  protected final Set<String> nonExistingRequestedTopicCache = new ConcurrentSkipListSet<>();
  protected final String kafkaClusterName;
  protected final Counter kafkaTopicsCounter = new Counter();
  protected final AtomicLong lastRefreshTime = new AtomicLong(0);
  protected final ObserverCallback observerCallback;
  protected final Set<String> partitionChangeWatcher;
  protected final Object partitionChangeWatcherLock;

  public TopicPartitionCountObserver(
      String kafkaClusterName,
      String clusterRootPath,
      String topicZkPath,
      int zkConnectionTimeoutMs,
      int zkSessionTimeoutMs,
      long refreshIntervalMs) {
    this(kafkaClusterName, new ZkClient(clusterRootPath, zkSessionTimeoutMs, zkConnectionTimeoutMs,
        ZKStringSerializer$.MODULE$), clusterRootPath, topicZkPath, refreshIntervalMs, null);
  }

  public TopicPartitionCountObserver(
      String kafkaClusterName,
      String clusterRootPath,
      String topicZkPath,
      int zkConnectionTimeoutMs,
      int zkSessionTimeoutMs,
      long refreshIntervalMs,
      ObserverCallback observerCallback) {
    this(kafkaClusterName, new ZkClient(clusterRootPath, zkSessionTimeoutMs, zkConnectionTimeoutMs,
        ZKStringSerializer$.MODULE$), clusterRootPath, topicZkPath, refreshIntervalMs, observerCallback);
  }

  @VisibleForTesting
  protected TopicPartitionCountObserver(
      String kafkaClusterName,
      ZkClient zkClient,
      String clusterRootPath,
      String topicZkPath,
      long refreshIntervalMs,
      ObserverCallback observerCallback) {
    super(refreshIntervalMs, TopicPartitionCountObserver.class.getSimpleName());
    this.kafkaClusterName = kafkaClusterName;
    this.zkClient = zkClient;
    this.clusterRootPath = clusterRootPath;
    this.zkUtils = ZkUtils.apply(zkClient, false);
    this.topicZkPath = topicZkPath;
    this.observerCallback = observerCallback;
    this.partitionChangeWatcher = new ConcurrentSkipListSet<>();
    this.partitionChangeWatcherLock = new Object();
  }

  public void start() {
    logger
        .info("Starting TopicPartitionCountObserver for zkConnect={}, zkPath={}", clusterRootPath,
            topicZkPath);
    super.start();
    zkClient.subscribeChildChanges(topicZkPath, this);
    registerMetric();
    logger.info("TopicPartitionCountObserver started");
  }

  public void shutdown() {
    logger.info("Shutting down TopicPartitionCountObserver for zkConnect={}, zkPath={}}",
        clusterRootPath, topicZkPath);
    super.shutdown();
    zkClient.unsubscribeChildChanges(topicZkPath, this);
    zkUtils.close();
    zkClient.close();
    logger.info("Shutdown TopicPartitionCountObserver finished");
  }

  private void registerMetric() {
    try {
      KafkaUReplicatorMetricsReporter.get().registerMetric(
          String.format(METRIC_TEMPLATE, kafkaClusterName, "kafkaTopicsCounter"), kafkaTopicsCounter);
    } catch (Exception e) {
      logger.error("Failed to register metrics to KafkaUReplicatorMetricsReporter ", e);
    }
  }

  @Override
  public void updateDataSet() {
    updateTopicPartitionInfoMap(topicPartitionMap.keySet());
    if (nonExistingRequestedTopicCache.size() != 0) {
      logger.warn("Couldn't find topics {}.", nonExistingRequestedTopicCache);
    }
    kafkaTopicsCounter.inc(topicPartitionMap.size() - kafkaTopicsCounter.getCount());
  }

  public int getPartitionCount(String topicName) {
    Integer partitionCount = topicPartitionMap.get(topicName);
    if (partitionCount != null) {
      if (nonExistingRequestedTopicCache.contains(topicName)) {
        nonExistingRequestedTopicCache.remove(topicName);
      }
      return partitionCount;
    } else {
      nonExistingRequestedTopicCache.add(topicName);
      return 0;
    }
  }


  public void addTopic(String topicName) {
    logger.info("Add topic={} to check partition count", topicName);
    topicPartitionMap.putIfAbsent(topicName, 0);
    updateTopicPartitionInfoMap(Collections.singleton(topicName));
  }

  /**
   * deleteTopic() is not safe because msg can be left in memory for consumption even after topic is removed. And the
   * cost to leave unused topics in the map should be negligible since the number of total topics is not large as for
   * now.
   */
  @Override
  public void handleChildChange(String s, List<String> currentChildren) throws Exception {
    if (shouldBackoff(lastRefreshTime.get())) {
      return;
    }
    logger
        .info("starting to refresh topic list due to zk child change, currentChildren size {}", currentChildren.size());
    final Set<String> topicsFromZookeeper = new HashSet<String>(currentChildren);
    Set<String> currentServingTopics = getAllTopics();
    Set<String> pendingRefreshTopics = pendingRefreshTopics(topicsFromZookeeper, currentServingTopics);
    if (pendingRefreshTopics.size() == 0) {
      return;
    }
    updateTopicPartitionInfoMap(pendingRefreshTopics);
    kafkaTopicsCounter.inc(topicPartitionMap.size() - kafkaTopicsCounter.getCount());
    logger.info("Update topics info by zk event for zkPath={}", topicZkPath);
    lastRefreshTime.set(System.currentTimeMillis());
  }

  boolean shouldBackoff(long lastRefreshTime) {
    return (lastRefreshTime + refreshIntervalMs) >= System.currentTimeMillis();
  }

  protected Set<String> pendingRefreshTopics(Set<String> topicsFromZookeeper, Set<String> currentServingTopics) {
    topicsFromZookeeper.retainAll(currentServingTopics);
    return topicsFromZookeeper;
  }

  protected void updateTopicPartitionInfoMap(final Set<String> topicsToCheck) {
    if (topicsToCheck.size() > 0) {
      // get topic partition count and maybe update partition counts for existing topics
      scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
          zkUtils.getPartitionAssignmentForTopics(
              JavaConversions.asScalaBuffer(ImmutableList.copyOf(topicsToCheck)));

      for (String topic : topicsToCheck) {
        if (partitionAssignmentForTopics.contains(topic)) {
          try {
            int currentPartitionNum = partitionAssignmentForTopics.get(topic).get().size();
            int oldPartitionNum = getPartitionCount(topic);
            if (oldPartitionNum != currentPartitionNum) {
              logger.info("Number of partition changed for topic {}, oldPartitionNum {}, newPartitionNumber {}", topic,
                  oldPartitionNum, currentPartitionNum);
              topicPartitionMap.put(topic, currentPartitionNum);
              if (partitionChangeWatcher.contains(topic)) {
                observerCallback.onPartitionNumberChange(topic, currentPartitionNum);
              }
            }
          } catch (Exception e) {
            logger.warn("Failed to get topicPartition info for topic={} of zkPath={}",
                topic, topicZkPath, e);
          }
          if (nonExistingRequestedTopicCache.contains(topic)) {
            nonExistingRequestedTopicCache.remove(topic);
          }
        } else {
          nonExistingRequestedTopicCache.add(topic);
        }
      }
    }
  }

  public TopicPartition getTopicPartition(String topicName) {
    int partitionCount = getPartitionCount(topicName);
    if (partitionCount > 0) {
      return new TopicPartition(topicName, partitionCount);
    } else {
      return null;
    }
  }

  public TopicPartition getTopicPartitionWithRefresh(String topicName) {
    TopicPartition topicPartition = getTopicPartition(topicName);
    if (topicPartition == null) {
      addTopic(topicName);
      return getTopicPartition(topicName);
    }
    return topicPartition;
  }

  public Set<String> getAllTopics() {
    return ImmutableSet.copyOf(topicPartitionMap.keySet());
  }

  public long getNumTopics() {
    return topicPartitionMap.size();
  }

  public void registerPartitionChangeWatcher(String topic) {
    TopicPartition partitionCount = getTopicPartitionWithRefresh(topic);
    if (partitionCount == null) {
      logger.warn("Skip register partition change watcher because of topic {} not found in cache", topic);
      return;
    }
    synchronized (partitionChangeWatcherLock) {
      logger.info("Register partition change watcher for topic {}, zkPath {}", topic, topicZkPath);
      this.zkClient.subscribeDataChanges(String.format("%s/%s", topicZkPath, topic), this);
      this.partitionChangeWatcher.add(topic);
    }
  }

  public void unsubscribePartitionChangeWatcher(String topic) {
    synchronized (partitionChangeWatcherLock) {
      logger.info("Unsubscribe partition change watcher for topic {}, zkPath {}", topic, topicZkPath);
      this.zkClient.unsubscribeDataChanges(
          String.format("%s/%s", topicZkPath, topic), this);
      this.partitionChangeWatcher.remove(topic);
    }
  }

  @Override
  public void handleDataChange(String s, Object o) throws Exception {
    logger.info("Received data change on path {}", s);
    String[] parts = s.split("/");
    String topicName = parts[parts.length - 1];
    updateTopicPartitionInfoMap(ImmutableSet.of(topicName));
  }

  @Override
  public void handleDataDeleted(String s) throws Exception {
    // do nothing
  }
}
