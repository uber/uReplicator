/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
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
  private final String _kakfaClusterName;
  private final Map<String, TopicPartition> _topicPartitionInfoMap =
      new ConcurrentHashMap<String, TopicPartition>();
  private final AtomicLong _lastRefreshTime = new AtomicLong(0);
  private final long _refreshTimeIntervalInMillis = 60 * 60 * 1000;
  private final Timer _refreshLatency = new Timer();
  private final Counter _kafkaTopicsCounter = new Counter();
  private final static String METRIC_TEMPLATE = "KafkaBrokerTopicObserver.%s.%s";
  private final Object _lock = new Object();

  public KafkaBrokerTopicObserver(String brokerClusterName, String zkString) {
    LOGGER.info("Trying to init KafkaBrokerTopicObserver {} with ZK: {}", brokerClusterName,
        zkString);
    _kakfaClusterName = brokerClusterName;
    _zkClient = new ZkClient(zkString, 30000, 30000, ZKStringSerializer$.MODULE$);
    _zkClient.subscribeChildChanges(KAFKA_TOPICS_PATH, this);
    registerMetric();
    executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        tryToRefreshCache();
      }
    }, 0, 600, TimeUnit.SECONDS);
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
        Set<String> newAddedTopics = new HashSet<String>(currentChilds);
        Set<String> currentServingTopics = getAllTopics();
        newAddedTopics.removeAll(currentServingTopics);
        for (String existedTopic : currentServingTopics) {
          if (!currentChilds.contains(existedTopic)) {
            _topicPartitionInfoMap.remove(existedTopic);
          }
        }
        scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
            ZkUtils.getPartitionAssignmentForTopics(_zkClient,
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
        _kafkaTopicsCounter.inc(_topicPartitionInfoMap.size() - _kafkaTopicsCounter.getCount());
      }
    }
  }

  private synchronized void refreshCache() {
    Context context = _refreshLatency.time();
    synchronized (_lock) {
      Set<String> servingTopics;
      try {
        servingTopics = ImmutableSet.copyOf(_zkClient.getChildren(KAFKA_TOPICS_PATH));
      } catch (Exception e) {
        LOGGER.warn("Failed to get topics from kafka zk: {}", e);
        return;
      }
      for (String existedTopic : getAllTopics()) {
        if (!servingTopics.contains(existedTopic)) {
          _topicPartitionInfoMap.remove(existedTopic);
        }
      }

      scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
          ZkUtils.getPartitionAssignmentForTopics(_zkClient,
              JavaConversions.asScalaBuffer(ImmutableList.copyOf(servingTopics)));

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

  private synchronized boolean tryToRefreshCache() {
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
