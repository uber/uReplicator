/*
 * Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * KafkaBrokerTopicObserver provides topic information on this broker
 * such as all topic names and partitions for each topic
 */
public class KafkaBrokerTopicObserver implements IZkChildListener {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaBrokerTopicObserver.class);

  private static String KAFKA_TOPICS_PATH = "/brokers/topics";

  private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

  private final ZkClient _zkClient;
  private final ZkUtils _zkUtils;
  private final String _kakfaClusterName;
  private final Map<String, TopicPartition> _topicPartitionInfoMap = new ConcurrentHashMap<>();
  private final AtomicLong _lastRefreshTime = new AtomicLong(0);
  private final long _refreshTimeIntervalInMillis = TimeUnit.MINUTES.toMillis(5);

  private final Timer _refreshLatency = new Timer();
  private final Counter _kafkaTopicsCounter = new Counter();
  private final static String METRIC_TEMPLATE = "KafkaBrokerTopicObserver.%s.%s";
  private final Object _lock = new Object();

  public KafkaBrokerTopicObserver(String brokerClusterName, String zkString, long refreshTimeIntervalInMillis) {
    LOGGER.info("Trying to init KafkaBrokerTopicObserver {} with ZK: {}", brokerClusterName,
            zkString);
    _kakfaClusterName = brokerClusterName;
    _refreshTimeIntervalInMillis = refreshTimeIntervalInMillis;
    _zkClient = new ZkClient(zkString, 30000, 30000, ZKStringSerializer$.MODULE$);
    _zkClient.subscribeChildChanges(KAFKA_TOPICS_PATH, this);
    _zkUtils = ZkUtils.apply(_zkClient, false);
    registerMetric();
    executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        tryToRefreshCache();
      }
    }, 0, _refreshTimeIntervalInMillis, TimeUnit.SECONDS);
  }

  private void registerMetric() {
    try {
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          String.format(METRIC_TEMPLATE, _kakfaClusterName, "refreshLatency"), _refreshLatency);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          String.format(METRIC_TEMPLATE, _kakfaClusterName, "kafkaTopicsCounter"),
          _kafkaTopicsCounter);
    } catch (Exception e) {
      LOGGER.error("Failed to register metrics to HelixKafkaMirrorMakerMetricsReporter " + e);
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
      throws Exception {
    if (!tryToRefreshCache()) {
      synchronized (_lock) {
        LOGGER.info("starting to refresh topic list due to zk child change");
        Set<String> newAddedTopics = new HashSet<>(currentChilds);
        Set<String> currentServingTopics = getAllTopics();
        newAddedTopics.removeAll(currentServingTopics);
        for (String existedTopic : currentServingTopics) {
          if (!currentChilds.contains(existedTopic)) {
            _topicPartitionInfoMap.remove(existedTopic);
          }
        }
        scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
            _zkUtils.getPartitionAssignmentForTopics(
                JavaConversions.asScalaBuffer(ImmutableList.copyOf(newAddedTopics)));

        for (String topic : newAddedTopics) {
          try {
            scala.collection.Map<Object, Seq<Object>> partitionsMap =
                partitionAssignmentForTopics.get(topic).get();
            TopicPartition tp = new TopicPartition(topic, partitionsMap.size());
            _topicPartitionInfoMap.put(topic, tp);
          } catch (Exception e) {
            LOGGER.warn("Failed to get topicPartition info for {} from kafka zk: {}", topic, e);
          }
        }
        LOGGER.info("added {} new topics to topic list in zk child change", newAddedTopics.size());
        _kafkaTopicsCounter.inc(_topicPartitionInfoMap.size() - _kafkaTopicsCounter.getCount());
      }
    }
  }

  private void tryAddTopic(String topic) {
    scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
        _zkUtils.getPartitionAssignmentForTopics(JavaConversions.asScalaBuffer(ImmutableList.of(topic)));
    if (partitionAssignmentForTopics.get(topic).isEmpty()
        || partitionAssignmentForTopics.get(topic).get().size() == 0) {
      LOGGER.info("try to refresh for topic {} but found no topic partition for it", topic);
      return;
    }
    synchronized (_lock) {
      LOGGER.info("starting to refresh for adding topic {}", topic);
      if (!getAllTopics().contains(topic)) {
        try {
          _topicPartitionInfoMap.put(topic, new TopicPartition(topic,
              partitionAssignmentForTopics.get(topic).get().size()));
        } catch (Exception e) {
          LOGGER.warn("Failed to get topicPartition info for {} from kafka zk: {}", topic, e);
        }
      }
      LOGGER.info("finished refreshing for adding topic {}", topic);
    }
  }

  private void refreshCache() {
    Context context = _refreshLatency.time();

    Set<String> servingTopics;
    try {
      servingTopics = ImmutableSet.copyOf(_zkClient.getChildren(KAFKA_TOPICS_PATH));
    } catch (Exception e) {
      LOGGER.warn("Failed to get topics from kafka zk: {}", e);
      return;
    }

    scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
        _zkUtils.getPartitionAssignmentForTopics(JavaConversions.asScalaBuffer(ImmutableList.copyOf(servingTopics)));

    synchronized (_lock) {
      LOGGER.info("updating topic cache map for {} new topics", servingTopics.size());
      for (String existedTopic : getAllTopics()) {
        if (!servingTopics.contains(existedTopic)) {
          _topicPartitionInfoMap.remove(existedTopic);
        }
      }

      for (String topic : servingTopics) {
        try {
          scala.collection.Map<Object, Seq<Object>> partitionsMap =
              partitionAssignmentForTopics.get(topic).get();
          TopicPartition tp = new TopicPartition(topic, partitionsMap.size());
          _topicPartitionInfoMap.put(topic, tp);
        } catch (Exception e) {
          LOGGER.warn("Failed to get topicPartition info for {} from kafka zk: {}", topic, e);
        }
      }
      LOGGER.info("Now serving {} topics in Kafka cluster: {}", _topicPartitionInfoMap.size(),
          _kakfaClusterName);
      _kafkaTopicsCounter.inc(_topicPartitionInfoMap.size() - _kafkaTopicsCounter.getCount());
      _lastRefreshTime.set(System.currentTimeMillis());
    }
    context.stop();
  }

  private boolean tryToRefreshCache() {
    if (_refreshTimeIntervalInMillis + _lastRefreshTime.get() < System.currentTimeMillis()) {
      refreshCache();
      return true;
    } else {
      LOGGER.debug("Not hitting next refresh interval, wait for the next run!");
      return false;
    }
  }

  public TopicPartition getTopicPartition(String topic) {
    TopicPartition topicPartition = _topicPartitionInfoMap.get(topic);
    if (topicPartition != null) {
      return new TopicPartition(topic, topicPartition.getPartition());
    } else {
      return null;
    }
  }

  public TopicPartition getTopicPartitionWithRefresh(String topic) {
    TopicPartition topicPartition = getTopicPartition(topic);
    if (topicPartition == null) {
      LOGGER.info("couldn't find topic {}, going to add topic and retry", topic);
      tryAddTopic(topic);
      LOGGER.info("refreshed and tried to fetch topic info again", topic);
      topicPartition = getTopicPartition(topic);
    }
    return topicPartition;
  }

  public Set<String> getAllTopics() {
    return ImmutableSet.copyOf(_topicPartitionInfoMap.keySet());
  }

  public long getNumTopics() {
    return _topicPartitionInfoMap.size();
  }

  public void stop() {
    _zkClient.unsubscribeChildChanges(KAFKA_TOPICS_PATH, this);
    executorService.shutdown();
  }

}
