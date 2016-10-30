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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Counter;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;

/**
 * Validate every one minute and update related metrics.
 */
public class ValidationManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ValidationManager.class);
  private static final String IDEALSTATE_PER_WORKER_METRICS_FORMAT =
      "idealStates.topicPartitions.%s.totalNumber";
  private static final String EXTERNALVIEW_PER_WORKER_METRICS_FORMAT =
      "externalView.topicPartitions.%s.totalNumber";

  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final ScheduledExecutorService _executorService =
      Executors.newSingleThreadScheduledExecutor();
  private int _timeValue = 60;
  private TimeUnit _timeUnit = TimeUnit.SECONDS;
  // Metrics
  private final Counter _isLeaderCounter = new Counter();

  private final Counter _numServingTopics = new Counter();
  private final Counter _numTopicPartitions = new Counter();
  private final Counter _numOnlineTopicPartitions = new Counter();
  private final Counter _numOfflineTopicPartitions = new Counter();
  private final Counter _numErrorTopicPartitions = new Counter();
  private final Counter _numErrorTopics = new Counter();

  private final Map<String, Counter> _idealStatePerWorkerTopicPartitionCounter =
      new HashMap<String, Counter>();
  private final Map<String, Counter> _externalViewPerWorkerTopicPartitionCounter =
      new HashMap<String, Counter>();

  public ValidationManager(HelixMirrorMakerManager helixMirrorMakerManager) {
    _helixMirrorMakerManager = helixMirrorMakerManager;
  }

  public void start() {
    registerMetrics();

    // Report current status every one minutes.
    LOGGER.info("Trying to schedule a validation job at rate {} {} !", _timeValue,
        _timeUnit.toString());
    _executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        if (_helixMirrorMakerManager.isLeader()) {
          _isLeaderCounter.inc(1 - _isLeaderCounter.getCount());
          LOGGER.info("Trying to run the validation job");
          validateExternalView();
        } else {
          cleanupMetrics();

          LOGGER.debug("Not leader, skip validation!");
        }
      }

      private void cleanupMetrics() {
        _isLeaderCounter.dec(_isLeaderCounter.getCount());
        _numServingTopics.dec(_numServingTopics.getCount());
        _numTopicPartitions.dec(_numTopicPartitions.getCount());
        _numOnlineTopicPartitions.dec(_numOnlineTopicPartitions.getCount());
        _numOfflineTopicPartitions.dec(_numOfflineTopicPartitions.getCount());
        _numErrorTopicPartitions.dec(_numErrorTopicPartitions.getCount());
        _numErrorTopics.dec(_numErrorTopics.getCount());
        for (Counter counter : _idealStatePerWorkerTopicPartitionCounter.values()) {
          counter.dec(counter.getCount());
        }
        for (Counter counter : _externalViewPerWorkerTopicPartitionCounter.values()) {
          counter.dec(counter.getCount());
        }

      }
    }, 120, _timeValue, _timeUnit);
  }

  private void registerMetrics() {
    try {
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("leader.counter",
          _isLeaderCounter);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("topic.totalNumber",
          _numServingTopics);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("topic.errorNumber",
          _numErrorTopics);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("topic.partitions.totalNumber",
          _numTopicPartitions);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("topic.partitions.onlineNumber",
          _numOnlineTopicPartitions);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("topic.partitions.offlineNumber",
          _numOfflineTopicPartitions);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("topic.partitions.errorNumber",
          _numErrorTopicPartitions);
    } catch (Exception e) {
      LOGGER.error("Error registering metrics!", e);
    }
  }

  public synchronized String validateExternalView() {
    try {
      Map<String, Integer> topicPartitionMapForIdealState =
          new HashMap<String, Integer>();
      Map<String, Integer> topicPartitionMapForExternalView =
          new HashMap<String, Integer>();
      int numOnlineTopicPartitions = 0;
      int numOfflineTopicPartitions = 0;
      int numErrorTopicPartitions = 0;
      int numTopicPartitions = 0;
      int numServingTopics = 0;
      int numErrorTopics = 0;
      for (String topicName : _helixMirrorMakerManager.getTopicLists()) {
        numServingTopics++;
        IdealState idealStateForTopic =
            _helixMirrorMakerManager.getIdealStateForTopic(topicName);
        ExternalView externalViewForTopic =
            _helixMirrorMakerManager.getExternalViewForTopic(topicName);
        numTopicPartitions += idealStateForTopic.getNumPartitions();
        if (idealStateForTopic.getNumPartitions() != externalViewForTopic.getPartitionSet()
            .size()) {
          numErrorTopics++;
          LOGGER.error(
              "For topic:{}, number of partitions for IdealState: {} doesn't match ExternalView: {}",
              topicName, idealStateForTopic, externalViewForTopic);
        }

        // IdealState Counting
        updateIdealstateInfo(topicPartitionMapForIdealState, idealStateForTopic);
        // ExternalView Counting
        for (String partition : externalViewForTopic.getPartitionSet()) {
          Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
          for (String instance : stateMap.keySet()) {
            String state = stateMap.get(instance);
            if (!topicPartitionMapForExternalView.containsKey(instance)) {
              topicPartitionMapForExternalView.put(instance, 1);
            } else {
              topicPartitionMapForExternalView.put(instance,
                  topicPartitionMapForExternalView.get(instance) + 1);
            }
            if ("ONLINE".equalsIgnoreCase(state)) {
              numOnlineTopicPartitions++;
            } else if ("OFFLINE".equalsIgnoreCase(state)) {
              numOfflineTopicPartitions++;
            } else if ("ERROR".equalsIgnoreCase(state)) {
              numErrorTopicPartitions++;
            }
          }
        }
      }

      if (_helixMirrorMakerManager.isLeader()) {
        updateMetrics(numOnlineTopicPartitions, numOfflineTopicPartitions, numErrorTopicPartitions,
            numTopicPartitions, numServingTopics, numErrorTopics);
        updatePerWorkerISMetrics(topicPartitionMapForExternalView);
        updatePerWorkerEVMetrics(topicPartitionMapForExternalView);
      }
      JSONObject perWorkerISCounterJson =
          constructPerWorkerISCounterJson(topicPartitionMapForIdealState);
      JSONObject perWorkerEVCounterJson =
          constructPerWorkerEVCounterJson(topicPartitionMapForExternalView);
      JSONObject validationResultJson = constructValidationResultJson(numOnlineTopicPartitions,
          numOfflineTopicPartitions, numErrorTopicPartitions, numTopicPartitions,
          numServingTopics, numErrorTopics, perWorkerISCounterJson, perWorkerEVCounterJson);
      return validationResultJson.toJSONString();
    } catch (Exception e) {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("excpetion", e);
      return jsonObject.toJSONString();
    }
  }

  private JSONObject constructValidationResultJson(int numOnlineTopicPartitions,
      int numOfflineTopicPartitions,
      int numErrorTopicPartitions, int numTopicPartitions, int numServingTopics, int numErrorTopics,
      JSONObject perWorkerISCounterJson, JSONObject perWorkerEVCounterJson) {
    JSONObject validationResultJson = new JSONObject();
    validationResultJson.put("numTopics", numServingTopics);
    validationResultJson.put("numTopicPartitions", numTopicPartitions);
    validationResultJson.put("numOnlineTopicPartitions", numOnlineTopicPartitions);
    validationResultJson.put("numOfflineTopicPartitions", numOfflineTopicPartitions);
    validationResultJson.put("numErrorTopicPartitions", numErrorTopicPartitions);
    validationResultJson.put("numErrorTopics", numErrorTopics);
    validationResultJson.put("IdealState", perWorkerISCounterJson);
    validationResultJson.put("ExternalView", perWorkerEVCounterJson);
    return validationResultJson;
  }

  private JSONObject constructPerWorkerISCounterJson(
      Map<String, Integer> topicPartitionMapForIdealState) {
    JSONObject perWorkISCounterJson = new JSONObject();
    for (String worker : topicPartitionMapForIdealState.keySet()) {
      perWorkISCounterJson.put(worker, topicPartitionMapForIdealState.get(worker));
    }
    return perWorkISCounterJson;
  }

  private JSONObject constructPerWorkerEVCounterJson(
      Map<String, Integer> topicPartitionMapForExternalView) {
    JSONObject perWorkEVCounterJson = new JSONObject();
    for (String worker : topicPartitionMapForExternalView.keySet()) {
      perWorkEVCounterJson.put(worker, topicPartitionMapForExternalView.get(worker));
    }
    return perWorkEVCounterJson;
  }

  private synchronized void updatePerWorkerISMetrics(
      Map<String, Integer> topicPartitionMapForIdealState) {
    for (String worker : topicPartitionMapForIdealState.keySet()) {
      if (!_idealStatePerWorkerTopicPartitionCounter.containsKey(worker)) {
        Counter workCounter = new Counter();
        try {
          HelixKafkaMirrorMakerMetricsReporter.get().getRegistry().register(
              getIdealStatePerWorkMetricName(worker), workCounter);
        } catch (Exception e) {
          LOGGER.error("Error registering metrics!", e);
        }
        _idealStatePerWorkerTopicPartitionCounter.put(worker, workCounter);
      }
      Counter counter = _idealStatePerWorkerTopicPartitionCounter.get(worker);
      counter.inc(topicPartitionMapForIdealState.get(worker) -
          counter.getCount());
    }
    for (String worker : _idealStatePerWorkerTopicPartitionCounter.keySet()) {
      if (!topicPartitionMapForIdealState.containsKey(worker)) {
        Counter counter = _idealStatePerWorkerTopicPartitionCounter.get(worker);
        counter.dec(counter.getCount());
      }
    }
  }

  private synchronized void updatePerWorkerEVMetrics(
      Map<String, Integer> topicPartitionMapForExternalView) {
    for (String worker : topicPartitionMapForExternalView.keySet()) {
      if (!_externalViewPerWorkerTopicPartitionCounter.containsKey(worker)) {
        Counter workCounter = new Counter();
        try {
          HelixKafkaMirrorMakerMetricsReporter.get().getRegistry().register(
              getExternalViewPerWorkMetricName(worker), workCounter);
        } catch (Exception e) {
          LOGGER.error("Error registering metrics!", e);
        }
        _externalViewPerWorkerTopicPartitionCounter.put(worker, workCounter);
      }
      Counter counter = _externalViewPerWorkerTopicPartitionCounter.get(worker);
      counter.inc(topicPartitionMapForExternalView.get(worker) -
          counter.getCount());
    }
    for (String worker : _externalViewPerWorkerTopicPartitionCounter.keySet()) {
      if (!topicPartitionMapForExternalView.containsKey(worker)) {
        Counter counter = _externalViewPerWorkerTopicPartitionCounter.get(worker);
        counter.dec(counter.getCount());
      }
    }
  }

  private synchronized void updateMetrics(int numOnlineTopicPartitions,
      int numOfflineTopicPartitions,
      int numErrorTopicPartitions, int numTopicPartitions, int numServingTopics,
      int numErrorTopics) {
    _numServingTopics
        .inc(numServingTopics - _numServingTopics.getCount());
    _numTopicPartitions
        .inc(numTopicPartitions - _numTopicPartitions.getCount());
    _numOnlineTopicPartitions
        .inc(numOnlineTopicPartitions - _numOnlineTopicPartitions.getCount());
    _numOfflineTopicPartitions
        .inc(numOfflineTopicPartitions - _numOfflineTopicPartitions.getCount());
    _numErrorTopicPartitions
        .inc(numErrorTopicPartitions - _numErrorTopicPartitions.getCount());
    _numErrorTopics
        .inc(numErrorTopics - _numErrorTopics.getCount());
  }

  private void updateIdealstateInfo(Map<String, Integer> topicPartitionMapForIdealState,
      IdealState idealStateForTopic) {
    for (String partition : idealStateForTopic.getPartitionSet()) {
      Map<String, String> idealStatesMap = idealStateForTopic.getInstanceStateMap(partition);
      for (String instance : idealStatesMap.keySet()) {
        if (!topicPartitionMapForIdealState.containsKey(instance)) {
          topicPartitionMapForIdealState.put(instance, 1);
        } else {
          topicPartitionMapForIdealState.put(instance,
              topicPartitionMapForIdealState.get(instance) + 1);
        }
      }
    }
  }

  private static String getIdealStatePerWorkMetricName(String worker) {
    return String.format(IDEALSTATE_PER_WORKER_METRICS_FORMAT, worker);
  }

  private static String getExternalViewPerWorkMetricName(String worker) {
    return String.format(EXTERNALVIEW_PER_WORKER_METRICS_FORMAT, worker);
  }
}
