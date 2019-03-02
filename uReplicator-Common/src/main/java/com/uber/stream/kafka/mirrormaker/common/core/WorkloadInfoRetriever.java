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
package com.uber.stream.kafka.mirrormaker.common.core;

import com.google.common.annotations.VisibleForTesting;
import com.uber.stream.kafka.mirrormaker.common.utils.C3QueryUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadInfoRetriever {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadInfoRetriever.class);

  @VisibleForTesting
  final Map<String, LinkedList<TopicWorkload>> _topicWorkloadMap = new ConcurrentHashMap<>();
  private TopicWorkload _defaultTopicWorkload;

  private final IHelixManager _helixMirrorMakerManager;
  private final String _srcKafkaCluster;
  private final String _simpleSrcKafkaCluster;

  private boolean _initialized = false;

  private final ScheduledExecutorService _periodicalScheduler = Executors.newSingleThreadScheduledExecutor();
  private final long _refreshPeriodInSeconds;
  private long _lastRefreshTimeMillis = 0;
  private final long _minRefreshIntervalMillis = 60000;

  private final String _c3Host;
  private final int _c3Port;
  private final boolean isController;

  private static final long DEFAULT_WORKLOAD_WINDOW_MILLIS = TimeUnit.MINUTES.toMillis(10);
  private static final long DEFAULT_WORKLOAD_COARSE_WINDOW_MILLIS = TimeUnit.HOURS.toMillis(3);

  // valid for a day so that it can be adaptive for traffic with day-pattern
  private long _maxValidTimeMillis = TimeUnit.HOURS.toMillis(25);
  private long _estimationLookBackWindow = TimeUnit.HOURS.toMillis(2);

  @VisibleForTesting
  WorkloadInfoRetriever() {
    _helixMirrorMakerManager = null;
    _srcKafkaCluster = "";
    _simpleSrcKafkaCluster = "";
    _refreshPeriodInSeconds = 300;
    _c3Host = "";
    _c3Port = 0;
    isController = false;
  }

  public WorkloadInfoRetriever(IHelixManager helixMirrorMakerManager, boolean isController) {
    this(helixMirrorMakerManager, isController, helixMirrorMakerManager.getConf().getSrcKafkaZkPath());
  }

  public WorkloadInfoRetriever(IHelixManager helixMirrorMakerManager, boolean isController, String srcKafkaZkPath) {
    this._helixMirrorMakerManager = helixMirrorMakerManager;
    this._refreshPeriodInSeconds = helixMirrorMakerManager.getConf().getWorkloadRefreshPeriodInSeconds();
    this._c3Host = helixMirrorMakerManager.getConf().getC3Host();
    this._c3Port = helixMirrorMakerManager.getConf().getC3Port();
    this.isController = isController;
    this._defaultTopicWorkload = new TopicWorkload(helixMirrorMakerManager.getConf().getBytesPerSecondDefault(),
        helixMirrorMakerManager.getConf().getMsgsPerSecondDefault());
    if (srcKafkaZkPath == null) {
      LOGGER.error("Source kafka Zookeeper path is not configured");
      _srcKafkaCluster = "";
      _simpleSrcKafkaCluster = "";
    } else {
      srcKafkaZkPath = srcKafkaZkPath.trim();
      int idx = srcKafkaZkPath.lastIndexOf('/');
      _srcKafkaCluster = idx < 0 ? "" : srcKafkaZkPath.substring(idx + 1);
      // if your route name is different from cluster name and route name is a postfix of cluster name
      _simpleSrcKafkaCluster = helixMirrorMakerManager.getConf().getClusterPrefixLength() == 0 ? _srcKafkaCluster :
          _srcKafkaCluster.substring(helixMirrorMakerManager.getConf().getClusterPrefixLength());
    }
  }

  public void start() {
    if (_srcKafkaCluster.isEmpty()) {
      LOGGER.error("Source kafka Zookeeper path is not configured. Skip to use workload retriever.");
      return;
    }
    LOGGER.info("Start workload retriever for {}", _srcKafkaCluster);

    if (_refreshPeriodInSeconds > 0) {
      // delay initialization for 0-5 minutes
      int delaySec = new Random().nextInt((int)_refreshPeriodInSeconds);
      LOGGER.info("Schedule periodical refreshing workload at rate {} seconds with delay {} seconds",
          _refreshPeriodInSeconds, delaySec);
      _periodicalScheduler.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          if (!_initialized) {
            try {
              initializeWorkloads();
              _initialized = true;
            } catch (Exception e) {
              LOGGER.error("Got exception during retrieve initial topic workloads! ", e);
            }
          } else {
            try {
              refreshWorkloads();
            } catch (Exception e) {
              LOGGER.error("Got exception during refresh topic workloads! ", e);
            }
          }
        }
      }, delaySec, _refreshPeriodInSeconds, TimeUnit.SECONDS);
    }
  }

  public void stop() {
    _periodicalScheduler.shutdown();
  }

  public boolean isInitialized() {
    return _initialized;
  }

  public TopicWorkload topicWorkload(String topic) {
    LinkedList<TopicWorkload> tws = _topicWorkloadMap.get(topic);
    if (tws == null || tws.isEmpty()) {
      return _defaultTopicWorkload;
    }
    // return the maximum bytes-in-rate during the valid window
    TopicWorkload maxTw = null;
    long current = System.currentTimeMillis();
    long lookbackWindow = _maxValidTimeMillis;
    if (tws.stream().anyMatch(topicWorkload -> {
      return current - topicWorkload.getLastUpdate() < _estimationLookBackWindow;
    })) {
      lookbackWindow = _estimationLookBackWindow;
    }
    for (TopicWorkload tw : tws) {
      if (current - tw.getLastUpdate() > lookbackWindow) {
        continue;
      }
      if (maxTw == null || maxTw.getBytesPerSecond() < tw.getBytesPerSecond()) {
        maxTw = tw;
      }
    }
    return (maxTw != null) ? maxTw : _defaultTopicWorkload;
  }

  public void setTopicDefaultWorkload(TopicWorkload defaultWorkload) {
    _defaultTopicWorkload = defaultWorkload;
  }


  public void refreshWorkloads() throws IOException {
    long current = System.currentTimeMillis();
    if (_lastRefreshTimeMillis + _minRefreshIntervalMillis > current) {
      LOGGER.info("Too soon to refresh workload, skip");
      return;
    }
    LOGGER.info("Refreshing workload for source " + _srcKafkaCluster);
    _lastRefreshTimeMillis = current;

    List<String> topics = _helixMirrorMakerManager.getTopicLists();
    Map<String, Integer> topicsPartitions = getTopicsPartitions(topics);
    retrieveWorkload(current, DEFAULT_WORKLOAD_WINDOW_MILLIS, topicsPartitions);
  }

  public void initializeWorkloads() throws IOException {
    List<String> topics = _helixMirrorMakerManager.getTopicLists();
    Map<String, Integer> topicsPartitions = getTopicsPartitions(topics);
    // use coarse granularity for the time windows older than 1 hour
    long current = System.currentTimeMillis();
    long fromMs = current - _maxValidTimeMillis;
    long toCoarseMs = current - TimeUnit.HOURS.toMillis(1);
    LOGGER.info("Initialize workload for source {} for time range [{}, {}]", _srcKafkaCluster, fromMs, current);
    _lastRefreshTimeMillis = current;
    for (long tsInMs = fromMs; tsInMs <= toCoarseMs; tsInMs += DEFAULT_WORKLOAD_COARSE_WINDOW_MILLIS) {
      retrieveWorkload(tsInMs, DEFAULT_WORKLOAD_COARSE_WINDOW_MILLIS, topicsPartitions);
    }
    // use fine granularity for the last hour
    for (long tsInMs = toCoarseMs + DEFAULT_WORKLOAD_WINDOW_MILLIS; tsInMs <= current;
         tsInMs += DEFAULT_WORKLOAD_WINDOW_MILLIS) {
      retrieveWorkload(tsInMs, DEFAULT_WORKLOAD_WINDOW_MILLIS, topicsPartitions);
    }
    LOGGER.info("Finished initializing workload for source " + _srcKafkaCluster);
  }

  private void retrieveWorkload(long timeInMs, long windowInMs, Map<String, Integer> topicsPartitions)
      throws IOException {
    long current = System.currentTimeMillis();
    Map<String, TopicWorkload> topicWorkloads = C3QueryUtils.retrieveTopicInRate(timeInMs, windowInMs,
        _c3Host, _c3Port, _srcKafkaCluster, new ArrayList<>(topicsPartitions.keySet()));
    LOGGER.info("Retrieved workload for ts: {} for srcKafkaCluster: {} and {} topics", timeInMs, _srcKafkaCluster, topicsPartitions.size());
    synchronized (_topicWorkloadMap) {
      for (Map.Entry<String, TopicWorkload> entry : topicWorkloads.entrySet()) {
        String topic = entry.getKey();
        TopicWorkload workload = entry.getValue();
        Integer partitions = topicsPartitions.get(topic);
        if (partitions != null) {
          workload.setParitions(partitions);
          LinkedList<TopicWorkload> tws = _topicWorkloadMap.get(topic);
          if (tws == null) {
            tws = new LinkedList<>();
            _topicWorkloadMap.put(topic, tws);
          }
          if (tws.isEmpty() || tws.getLast().getLastUpdate() < workload.getLastUpdate()) {
            tws.add(workload);
          }
          // purge the data points out of the valid window
          while (!tws.isEmpty() && (current - tws.getFirst().getLastUpdate() > _maxValidTimeMillis)) {
            tws.removeFirst();
          }
        }
      }
    }
  }

  private Map<String, Integer> getTopicsPartitions(List<String> topics) {
    Map<String, Integer> topicsPartitions = new HashMap<>();
    for (String topic : topics) {
      IdealState idealState = _helixMirrorMakerManager.getIdealStateForTopic(topic);
      // TODO: make it compatible with controller
      if (idealState != null) {
        if (isController) {
          int partitions = idealState.getNumPartitions();
          if (partitions > 0) {
            topicsPartitions.put(topic, partitions);
          }
        } else {
          Iterator<String> iter = idealState.getPartitionSet().iterator();
          while (iter.hasNext()) {
            String route = iter.next();
            if (route.substring(1).startsWith(_simpleSrcKafkaCluster)) {
              topicsPartitions.put(topic, idealState.getNumPartitions());
              break;
            }
          }
        }
      }
    }
    return topicsPartitions;
  }
}
