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
package com.uber.stream.kafka.mirrormaker.controller.validation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Counter;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.controller.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;

/**
 * Validate idealstates and source kafka cluster info and update related metrics.
 */
public class SourceKafkaClusterValidationManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SourceKafkaClusterValidationManager.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final ScheduledExecutorService _executorService =
      Executors.newSingleThreadScheduledExecutor();
  private int _timeValue = 60 * 10;
  private TimeUnit _timeUnit = TimeUnit.SECONDS;
  // Metrics
  private final static String MISMATCHED_METRICS_FORMAT =
      "kafkaAndIdealstatesDiscrepancy.mismatched.topic.%s";
  private final Counter _numMissingTopics = new Counter();
  private final Counter _numMismatchedTopics = new Counter();
  private final Counter _numMismatchedTopicPartitions = new Counter();
  private final KafkaBrokerTopicObserver _sourceKafkaTopicObserver;
  private final boolean _enableAutoTopicExpansion;
  private final Counter _numAutoExpandedTopics = new Counter();
  private final Counter _numAutoExpandedTopicPartitions = new Counter();

  private final Map<String, Counter> _mismatchedTopicPartitionsCounter =
      new HashMap<String, Counter>();

  public SourceKafkaClusterValidationManager(KafkaBrokerTopicObserver sourceKafkaTopicObserver,
      HelixMirrorMakerManager helixMirrorMakerManager) {
    this(sourceKafkaTopicObserver, helixMirrorMakerManager, false);
  }

  public SourceKafkaClusterValidationManager(KafkaBrokerTopicObserver sourceKafkaTopicObserver,
      HelixMirrorMakerManager helixMirrorMakerManager, boolean enableAutoTopicExpansion) {
    _sourceKafkaTopicObserver = sourceKafkaTopicObserver;
    _helixMirrorMakerManager = helixMirrorMakerManager;
    _enableAutoTopicExpansion = enableAutoTopicExpansion;
  }

  public void start() {
    registerMetrics();

    // Report current status every one minutes.
    LOGGER.info("Trying to schedule a source kafka cluster validation job at rate {} {} !",
        _timeValue, _timeUnit.toString());
    _executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        if (_helixMirrorMakerManager.isLeader()) {
          LOGGER.info("Trying to run the source kafka cluster info validation job");
          validateSourceKafkaCluster();
        } else {
          cleanupMetrics();
          LOGGER.debug("Not leader, skip validation for source kafka cluster!");
        }

      }

      private void cleanupMetrics() {
        _numMissingTopics.dec(_numMissingTopics.getCount());
        _numMismatchedTopics.dec(_numMismatchedTopics.getCount());
        _numMismatchedTopicPartitions.dec(_numMismatchedTopicPartitions.getCount());

        for (String topic : _mismatchedTopicPartitionsCounter.keySet()) {
          Counter counter = _mismatchedTopicPartitionsCounter.get(topic);
          counter.dec(counter.getCount());
        }
      }
    }, 120, _timeValue, _timeUnit);
  }

  public String validateSourceKafkaCluster() {
    Set<String> notExistedTopics = new HashSet<String>();
    Map<String, Integer> misMatchedPartitionNumberTopics = new HashMap<String, Integer>();
    int numMismatchedTopicPartitions = 0;
    for (String topic : _helixMirrorMakerManager.getTopicLists()) {
      TopicPartition tp = _sourceKafkaTopicObserver.getTopicPartition(topic);
      if (tp == null) {
        LOGGER.warn("Topic {} is not in source kafka broker!", topic);
        notExistedTopics.add(topic);
      } else {
        int numPartitionsInMirrorMaker =
            _helixMirrorMakerManager.getIdealStateForTopic(topic).getNumPartitions();
        if (numPartitionsInMirrorMaker != tp.getPartition()) {
          int mismatchedPartitions = Math.abs(numPartitionsInMirrorMaker - tp.getPartition());
          if (_enableAutoTopicExpansion && (tp.getPartition() > numPartitionsInMirrorMaker)) {
            // Only do topic expansion
            LOGGER.warn(
                "Trying to expand topic {} from {} partitions in mirror maker to {} from source kafka broker!",
                topic, numPartitionsInMirrorMaker, tp.getPartition());
            _numAutoExpandedTopics.inc();
            _numAutoExpandedTopicPartitions.inc(mismatchedPartitions);
            _helixMirrorMakerManager.expandTopicInMirrorMaker(tp);
          } else {
            numMismatchedTopicPartitions += mismatchedPartitions;
            misMatchedPartitionNumberTopics.put(topic, mismatchedPartitions);
            LOGGER.warn(
                "Number of partitions not matched for topic {} between mirrormaker:{} and source kafka broker: {}!",
                topic, numPartitionsInMirrorMaker, tp.getPartition());
          }
        }
      }
    }
    JSONObject mismatchedTopicPartitionsJson =
        constructMismatchedTopicPartitionsJson(misMatchedPartitionNumberTopics);
    JSONObject validationResultJson = constructValidationResultJson(notExistedTopics.size(),
        misMatchedPartitionNumberTopics.size(),
        numMismatchedTopicPartitions, mismatchedTopicPartitionsJson);
    if (_helixMirrorMakerManager.isLeader()) {
      updateMetrics(notExistedTopics.size(), misMatchedPartitionNumberTopics.size(),
          numMismatchedTopicPartitions, misMatchedPartitionNumberTopics);
    }
    return validationResultJson.toJSONString();
  }

  private void registerMetrics() {
    try {
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          "kafkaAndIdealstatesDiscrepancy.missing.topics",
          _numMissingTopics);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          "kafkaAndIdealstatesDiscrepancy.mismatched.topics",
          _numMismatchedTopics);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          "kafkaAndIdealstatesDiscrepancy.mismatched.topicPartitions",
          _numMismatchedTopicPartitions);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          "kafkaAndIdealstatesDiscrepancy.autoExpansion.topics",
          _numAutoExpandedTopics);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          "kafkaAndIdealstatesDiscrepancy.autoExpansion.topicPartitions",
          _numAutoExpandedTopicPartitions);
    } catch (Exception e) {
      LOGGER.error("Failed to register metrics to HelixKafkaMirrorMakerMetricsReporter " + e);
    }
  }

  private JSONObject constructValidationResultJson(int numMissingTopics, int numMismatchedTopics,
      int numMismatchedTopicPartitions, JSONObject mismatchedTopicPartitions) {
    JSONObject validationResultJson = new JSONObject();
    validationResultJson.put("numMissingTopics", numMissingTopics);
    validationResultJson.put("numMismatchedTopics", numMismatchedTopics);
    validationResultJson.put("numMismatchedTopicPartitions", numMismatchedTopicPartitions);
    validationResultJson.put("mismatchedTopicPartitions", mismatchedTopicPartitions);
    return validationResultJson;
  }

  private JSONObject constructMismatchedTopicPartitionsJson(
      Map<String, Integer> mismatchedTopicPartitions) {
    JSONObject mismatchedTopicPartitionsJson = new JSONObject();
    for (String topic : mismatchedTopicPartitions.keySet()) {
      mismatchedTopicPartitionsJson.put(topic, mismatchedTopicPartitions.get(topic));
    }
    return mismatchedTopicPartitionsJson;
  }

  private synchronized void updateMetrics(int numMissingTopics, int numMismatchedTopics,
      int numMismatchedTopicPartitions, Map<String, Integer> misMatchedPartitionNumberTopics) {
    _numMissingTopics.inc(numMissingTopics - _numMissingTopics.getCount());
    _numMismatchedTopics.inc(numMismatchedTopics - _numMismatchedTopics.getCount());
    _numMismatchedTopicPartitions
        .inc(numMismatchedTopicPartitions - _numMismatchedTopicPartitions.getCount());

    for (String topic : misMatchedPartitionNumberTopics.keySet()) {
      if (!_mismatchedTopicPartitionsCounter.containsKey(topic)) {
        Counter topicPartitionCounter = new Counter();
        try {
          HelixKafkaMirrorMakerMetricsReporter.get().getRegistry().register(
              getMismatchedTopicMetricName(topic), topicPartitionCounter);
        } catch (Exception e) {
          LOGGER.error("Error registering metrics!", e);
        }
        _mismatchedTopicPartitionsCounter.put(topic, topicPartitionCounter);
      }
    }
    for (String topic : _mismatchedTopicPartitionsCounter.keySet()) {
      Counter counter = _mismatchedTopicPartitionsCounter.get(topic);
      if (!misMatchedPartitionNumberTopics.containsKey(topic)) {
        counter.dec(counter.getCount());
      } else {
        counter.inc(misMatchedPartitionNumberTopics.get(topic) - counter.getCount());
      }
    }
  }

  private static String getMismatchedTopicMetricName(String topicName) {
    return String.format(MISMATCHED_METRICS_FORMAT, topicName.replace(".", "_"));
  }

}
