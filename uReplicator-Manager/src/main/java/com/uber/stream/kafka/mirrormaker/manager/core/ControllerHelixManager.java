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
package com.uber.stream.kafka.mirrormaker.manager.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Counter;
import com.google.common.net.HostAndPort;
import com.uber.stream.kafka.mirrormaker.common.configuration.IuReplicatorConf;
import com.uber.stream.kafka.mirrormaker.common.core.IHelixManager;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.modules.ControllerWorkloadInfo;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HttpClientUtils;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.validation.KafkaClusterValidationManager;

import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.*;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic for Helix Manager-Controller
 *
 * @author hongxu
 */
public class ControllerHelixManager implements IHelixManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerHelixManager.class);

  private static final String MANAGER_CONTROLLER_HELIX_PREFIX = "manager-controller";
  private static final String CONFIG_KAFKA_CLUSTER_KEY_PREFIX = "kafka.cluster.zkStr.";
  private static final String SEPARATOR = "@";

  private final ManagerConf _conf;
  private final KafkaClusterValidationManager _kafkaValidationManager;
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private ZkHelixPropertyStore<ZNRecord> _helixPropertyStore;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final WorkerHelixManager _workerHelixManager;
  private LiveInstanceChangeListener _liveInstanceChangeListener;
  private Map<String, TopicWorkload> _pipelineWorkloadMap;

  private final CloseableHttpClient _httpClient;
  private final RequestConfig _requestConfig;

  private final int _workloadRefreshPeriodInSeconds;
  private final int _initMaxNumPartitionsPerRoute;
  private final int _maxNumPartitionsPerRoute;
  private final int _initMaxNumWorkersPerRoute;
  private final int _maxNumWorkersPerRoute;

  private Map<String, Map<String, Counter>> _routeToCounterMap;
  private static final String TOPIC_TOTAL_NUMBER = "topicTotalNumber";
  private static final String TOPIC_ERROR_NUMBER = "topicErrorNumber";
  private static final String CONTROLLER_TOTAL_NUMBER = "controllerTotalNumber";
  private static final String CONTROLLER_ERROR_NUMBER = "controllerErrorNumber";
  private static final String WORKER_TOTAL_NUMBER = "workerTotalNumber";
  private static final String WORKER_ERROR_NUMBER = "workerErrorNumber";
  private static final String WORKER_NUMBER_OVERRIDE = "worker_number_override";
  private static final String PIPELINE_PATH = "/pipeline";

  private static final Counter _availableController = new Counter();
  private static final Counter _availableWorker = new Counter();
  private static final Counter _nonParityTopic = new Counter();
  private static final Counter _validateWrongCount = new Counter();
  private static final Counter _rescaleFailedCount = new Counter();
  private static final Counter _lowUrgencyValidateWrongCount = new Counter();
  private static final Counter _assignedControllerCount = new Counter();

  private static final int _numOfWorkersBatchSize = 5;

  private ReentrantLock _lock = new ReentrantLock();
  private Map<String, Map<String, InstanceTopicPartitionHolder>> _topicToPipelineInstanceMap;
  private Map<String, PriorityQueue<InstanceTopicPartitionHolder>> _pipelineToInstanceMap;
  private List<String> _availableControllerList;

  private long lastUpdateTimeMs = 0L;

  private ZkClient _zkClient;

  private boolean _enableAutoScaling = true;
  private boolean _enableRebalance;

  public ControllerHelixManager(
      KafkaClusterValidationManager kafkaValidationManager,
      ManagerConf managerConf) {
    _conf = managerConf;
    _enableRebalance = managerConf.getEnableRebalance();
    _kafkaValidationManager = kafkaValidationManager;
    _initMaxNumPartitionsPerRoute = managerConf.getInitMaxNumPartitionsPerRoute();
    _maxNumPartitionsPerRoute = managerConf.getMaxNumPartitionsPerRoute();
    _initMaxNumWorkersPerRoute = managerConf.getInitMaxNumWorkersPerRoute();
    _maxNumWorkersPerRoute = managerConf.getMaxNumWorkersPerRoute();
    _workloadRefreshPeriodInSeconds = managerConf.getWorkloadRefreshPeriodInSeconds();
    _workerHelixManager = new WorkerHelixManager(managerConf);
    _pipelineWorkloadMap = new ConcurrentHashMap<>();
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(managerConf.getManagerZkStr());
    _helixClusterName = MANAGER_CONTROLLER_HELIX_PREFIX + "-" + managerConf.getManagerDeployment();
    _instanceId = managerConf.getManagerInstanceId();
    _topicToPipelineInstanceMap = new ConcurrentHashMap<>();
    _pipelineToInstanceMap = new ConcurrentHashMap<>();
    _availableControllerList = new ArrayList<>();
    _routeToCounterMap = new ConcurrentHashMap<>();
    _zkClient = new ZkClient(_helixZkURL, 30000, 30000, ZKStringSerializer$.MODULE$);
    registerMetrics();

    PoolingHttpClientConnectionManager limitedConnMgr = new PoolingHttpClientConnectionManager();
    // TODO: make it configurable
    limitedConnMgr.setDefaultMaxPerRoute(100);
    limitedConnMgr.setMaxTotal(100);
    _httpClient = HttpClients.createMinimal(limitedConnMgr);
    // requestConfig is immutable. These three timeouts are for
    // 1. getting connection from connection manager;
    // 2. establishing connection with server;
    // 3. getting next data snippet from server.
    _requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(30000)
        .setConnectTimeout(30000)
        .setSocketTimeout(30000)
        .build();
  }

  public synchronized void start() {
    LOGGER.info("Trying to start ManagerControllerHelix!");

    _workerHelixManager.start();

    _helixManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixManager.getClusterManagmentTool();
    _helixPropertyStore = _helixManager.getHelixPropertyStore();

    updateCurrentStatus();

    LOGGER.info("Trying to register ControllerLiveInstanceChangeListener");
    _liveInstanceChangeListener = new ControllerLiveInstanceChangeListener(this, _helixManager, _workloadRefreshPeriodInSeconds);
    try {
      _helixManager.addLiveInstanceChangeListener(_liveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add ControllerLiveInstanceChangeListener");
    }
  }

  public synchronized void stop() throws IOException {
    LOGGER.info("Trying to stop ManagerControllerHelix!");
    _zkClient.close();
    _workerHelixManager.stop();
    _helixManager.disconnect();
    _httpClient.close();
  }

  private void registerMetrics() {
    try {
      KafkaUReplicatorMetricsReporter.get().registerMetric("controller.available.counter",
          _availableController);
      KafkaUReplicatorMetricsReporter.get().registerMetric("worker.available.counter",
          _availableWorker);
      KafkaUReplicatorMetricsReporter.get().registerMetric("topic.non-parity.counter",
          _nonParityTopic);
      KafkaUReplicatorMetricsReporter.get().registerMetric("validate.wrong.counter",
          _validateWrongCount);
      KafkaUReplicatorMetricsReporter.get().registerMetric("rescale.failed.counter",
          _rescaleFailedCount);
      KafkaUReplicatorMetricsReporter.get().registerMetric("validate.wrong.low.urgency.counter",
          _lowUrgencyValidateWrongCount);
      KafkaUReplicatorMetricsReporter.get().registerMetric("controller.assigned.counter",
          _assignedControllerCount);
    } catch (Exception e) {
      LOGGER.error("Error registering metrics!", e);
    }
  }

  private void maybeRegisterMetrics(String route) {
    if (!_routeToCounterMap.containsKey(route)) {
      _routeToCounterMap.putIfAbsent(route, new ConcurrentHashMap<>());
      _routeToCounterMap.get(route).put(TOPIC_TOTAL_NUMBER, new Counter());
      //_routeToCounterMap.get(routeString).put(TOPIC_ERROR_NUMBER, new Counter());
      _routeToCounterMap.get(route).put(CONTROLLER_TOTAL_NUMBER, new Counter());
      //_routeToCounterMap.get(routeString).put(CONTROLLER_ERROR_NUMBER, new Counter());
      _routeToCounterMap.get(route).put(WORKER_TOTAL_NUMBER, new Counter());
      //_routeToCounterMap.get(routeString).put(WORKER_ERROR_NUMBER, new Counter());
      try {
        KafkaUReplicatorMetricsReporter.get().registerMetric(route + ".topic.totalNumber",
            _routeToCounterMap.get(route).get(TOPIC_TOTAL_NUMBER));
        //HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(routeString + ".topic.errorNumber",
        //    _routeToCounterMap.get(routeString).get(TOPIC_ERROR_NUMBER));
        KafkaUReplicatorMetricsReporter.get().registerMetric(route + ".controller.totalNumber",
            _routeToCounterMap.get(route).get(CONTROLLER_TOTAL_NUMBER));
        //HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(routeString + "controller.errorNumber",
        //    _routeToCounterMap.get(routeString).get(CONTROLLER_ERROR_NUMBER));
        KafkaUReplicatorMetricsReporter.get().registerMetric(route + ".worker.totalNumber",
            _routeToCounterMap.get(route).get(WORKER_TOTAL_NUMBER));
        //HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(routeString + "worker.errorNumber",
        //    _routeToCounterMap.get(routeString).get(WORKER_ERROR_NUMBER));
      } catch (Exception e) {
        LOGGER.error("Error registering metrics!", e);
      }
    }
  }

  private String convert(String route) {
    return route.replace('@', '-').substring(1);
  }

  private void validateInstanceToTopicPartitionsMap(
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap,
      Map<String, InstanceTopicPartitionHolder> instanceMap) {
    LOGGER.info("validateInstanceToTopicPartitionsMap()");
    int validateWrongCount = 0;
    int lowUrgencyValidateWrongCount = 0;
    for (String instanceId : instanceToTopicPartitionsMap.keySet()) {
      HostAndPort hostInfo = null;
      try {
        hostInfo = getHostInfo(instanceId);
      } catch (ControllerException ex){
          LOGGER.error("Validate WRONG: Trying to get hostInfo for InstanceId: {} failed ", instanceId);
      }
      Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceId);
      Set<TopicPartition> routeSet = new HashSet<>();
      // TODO: one instance suppose to have only one route
      for (TopicPartition tp : topicPartitions) {
        String topicName = tp.getTopic();
        if (topicName.startsWith(SEPARATOR)) {
          routeSet.add(tp);
        }
      }

      if (routeSet.size() != 1) {
        Set<String> topicRouteSet = new HashSet<>();
        for (TopicPartition tp : topicPartitions) {
          String topicName = tp.getTopic();
          if (!topicName.startsWith(SEPARATOR)) {
            topicRouteSet.add(tp.getPipeline());
          }
        }
        validateWrongCount++;
        LOGGER.error("Validate WRONG: Incorrect route found for hostInfo: {}, InstanceId: {}, route: {}, pipelines: {}, #workers: {}, worker: {}",
                hostInfo, instanceId, routeSet, topicRouteSet, instanceMap.get(instanceId).getWorkerSet().size(), instanceMap.get(instanceId).getWorkerSet());
      } else {
        int partitionCount = 0;
        Set<TopicPartition> mismatchTopicPartition = new HashSet<>();
        TopicPartition route = routeSet.iterator().next();
        String routeString = route.getTopic() + SEPARATOR + route.getPartition();
        for (TopicPartition tp : topicPartitions) {
          String topicName = tp.getTopic();
          if (!topicName.startsWith(SEPARATOR)) {
            partitionCount += tp.getPartition();
            if (!tp.getPipeline().equals(routeString)) {
              mismatchTopicPartition.add(tp);
            }
          }
        }
        if (mismatchTopicPartition.isEmpty() && hostInfo != null) {
          LOGGER.info("Validate OK: hostInfo: {}, InstanceId: {}, route: {}, #topics: {}, #partitions: {}, #workers: {}, worker: {}", hostInfo, instanceId, routeSet,
              topicPartitions.size() - 1, partitionCount, instanceMap.get(instanceId).getWorkerSet().size(), instanceMap.get(instanceId).getWorkerSet());

          try {
            // try find topic mismatch between manager and controller
            String topicResult = HttpClientUtils.getData(_httpClient, _requestConfig,
                    hostInfo.getHost(), hostInfo.getPort(), "/topics");
            LOGGER.debug("Get topics from {}: {}", hostInfo, topicResult);
            String rawTopicNames = topicResult;
            if (!rawTopicNames.equals("No topic is added in MirrorMaker Controller!")) {
              rawTopicNames = topicResult.substring(25, topicResult.length() - 1);
            }
            Set<String> controllerTopics = new HashSet<>();
            if (!rawTopicNames.equals("No topic is added in MirrorMaker Controller!")) {
              String[] topicNames = rawTopicNames.split(", ");
              for (String name : topicNames) {
                controllerTopics.add(name);
              }
            }

            Set<String> topicOnlyInManager = new HashSet<>();
            for (TopicPartition tp : topicPartitions) {
              if (!controllerTopics.contains(tp.getTopic())) {
                topicOnlyInManager.add(tp.getTopic());
              } else {
                controllerTopics.remove(tp.getTopic());
              }
            }

            if (topicOnlyInManager.size() > 1 || (topicOnlyInManager.size() == 1 && !topicOnlyInManager.iterator().next().startsWith(SEPARATOR))) {
              validateWrongCount++;
              LOGGER.error("Validate WRONG: hostInfo: {}, InstanceId: {}, route: {}, topic only in manager: {}", hostInfo, instanceId, routeSet, topicOnlyInManager);
            }

            if (!controllerTopics.isEmpty()) {
              validateWrongCount++;
              LOGGER.error("Validate WRONG: hostInfo: {}, InstanceId: {}, route: {}, topic only in controller: {}", hostInfo, instanceId, routeSet, controllerTopics);
            }
          } catch (Exception e) {
            validateWrongCount++;
            LOGGER.error("Validate WRONG: Get topics error when connecting to {} for route {}", hostInfo, routeSet, e);
          }

          try {
            // try find worker mismatch between manager and controller
            String instanceResult = HttpClientUtils.getData(_httpClient, _requestConfig,
                    hostInfo.getHost(), hostInfo.getPort(), "/instances");
            LOGGER.debug("Get workers from {}: {}", hostInfo, instanceResult);
            JSONObject instanceResultJson = JSON.parseObject(instanceResult);
            JSONArray allInstances = instanceResultJson.getJSONArray("allInstances");
            Set<String> controllerWorkers = new HashSet<>();
            for (Object instance : allInstances) {
              controllerWorkers.add(String.valueOf(instance));
            }

            Set<String> managerWorkers = instanceMap.get(instanceId).getWorkerSet();
            Set<String> workerOnlyInManager = new HashSet<>();
            for (String worker : managerWorkers) {
              if (!controllerWorkers.contains(worker)) {
                workerOnlyInManager.add(worker);
              } else {
                controllerWorkers.remove(worker);
              }
            }

            if (!workerOnlyInManager.isEmpty()) {
              lowUrgencyValidateWrongCount++;
              LOGGER.warn("Validate WRONG: hostInfo: {}, InstanceId: {}, route: {}, worker only in manager: {}", hostInfo, instanceId, routeSet, workerOnlyInManager);
            }

            if (!controllerWorkers.isEmpty()) {
              validateWrongCount++;
              LOGGER.error("Validate WRONG: hostInfo: {}, InstanceId: {}, route: {}, worker only in controller: {}", hostInfo, instanceId, routeSet, controllerWorkers);
            }
          } catch (Exception e) {
            validateWrongCount++;
            LOGGER.error("Validate WRONG: Get workers error when connecting to {} for route {}", hostInfo, routeSet, e);
          }

        } else if (hostInfo == null) {
          validateWrongCount++;
        } else {
          validateWrongCount++;
          LOGGER.error("Validate WRONG: mismatch route found for hostInfo: {}, InstanceId: {}, route: {}, mismatch: {}, #workers: {}, worker: {}",
                  hostInfo, instanceId, routeSet, mismatchTopicPartition, instanceMap.get(instanceId).getWorkerSet().size(), instanceMap.get(instanceId).getWorkerSet());
        }
      }
    }
    Map<String, Set<String>> topicToRouteMap = new HashMap<>();
    for (String instanceId : instanceToTopicPartitionsMap.keySet()) {
      Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceId);
      Set<TopicPartition> routeSet = new HashSet<>();
      // TODO: one instance suppose to have only one route
      for (TopicPartition tp : topicPartitions) {
        String topicName = tp.getTopic();
        if (topicName.startsWith(SEPARATOR)) {
          routeSet.add(tp);
        }
      }
      TopicPartition route = routeSet.iterator().next();
      String routeString = route.getTopic() + SEPARATOR + route.getPartition();
      for (TopicPartition tp : topicPartitions) {
        String topicName = tp.getTopic();
        if (!topicName.startsWith(SEPARATOR)) {
          if (!topicToRouteMap.containsKey(topicName)) {
            topicToRouteMap.put(topicName, new HashSet<>());
            topicToRouteMap.get(topicName).add(routeString);
          } else {
            Set<String> existingRouteSet = topicToRouteMap.get(topicName);
            Iterator<String> iter = existingRouteSet.iterator();
            while (iter.hasNext()) {
              String existingRoute = iter.next();
              if (existingRoute.split(SEPARATOR)[0].equals(routeString.split(SEPARATOR)[0])) {
                iter.remove();
              }
            }
            if (existingRouteSet.isEmpty()) {
              topicToRouteMap.remove(topicName);
            }
          }
        }
      }
    }
    LOGGER.info("Non-parity topicToRouteMap: {}", topicToRouteMap);
    if (_helixManager.isLeader()) {
      _nonParityTopic.inc(topicToRouteMap.size() - _nonParityTopic.getCount());
    }

    LOGGER.info("For controller _pipelineToInstanceMap:");
    Map<String, Set<String>> workerMap = new HashMap<>();
    for (String pipeline : _pipelineToInstanceMap.keySet()) {
      PriorityQueue<InstanceTopicPartitionHolder> itphSet = _pipelineToInstanceMap.get(pipeline);
      for (InstanceTopicPartitionHolder itph : itphSet) {
        Set<String> workers = itph.getWorkerSet();
        for (String worker : workers) {
          if (workerMap.containsKey(worker)) {
            workerMap.get(worker).add(itph.getRouteString());
          } else {
            Set<String> routeSet = new HashSet<>();
            routeSet.add(itph.getRouteString());
            workerMap.put(worker, routeSet);
          }
        }
      }
    }
    for (String worker : workerMap.keySet()) {
      if (workerMap.get(worker).size() != 1) {
        validateWrongCount++;
        LOGGER.error("Validate WRONG: wrong worker assignment for worker: {}, route: {}", worker, workerMap.get(worker));
      }
    }
    if (_helixManager.isLeader()) {
      _validateWrongCount.inc(validateWrongCount - _validateWrongCount.getCount());
      _lowUrgencyValidateWrongCount.inc(lowUrgencyValidateWrongCount - _lowUrgencyValidateWrongCount.getCount());
      updateMetrics(instanceToTopicPartitionsMap, instanceMap);
    }
  }

  private void updateMetrics(
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap,
      Map<String, InstanceTopicPartitionHolder> instanceMap) {
    // int[3]: 0: #topic, 1: #controller, 2: #worker
    Map<String, int[]> currRouteInfo = new ConcurrentHashMap<>();
    //LOGGER.info("instanceToTopicPartitionsMap: {}", instanceToTopicPartitionsMap);
    for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
      Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
      for (TopicPartition tp : topicPartitions) {
        String topicName = tp.getTopic();
        if (topicName.startsWith(SEPARATOR)) {
          // route
          String route = topicName + SEPARATOR + tp.getPartition();
          String routeString = convert(route);
          currRouteInfo.putIfAbsent(routeString, new int[3]);
          currRouteInfo.get(routeString)[1]++;
          currRouteInfo.get(routeString)[2] += instanceMap.get(instanceName).getWorkerSet().size();

          // register metrics if needed
          maybeRegisterMetrics(routeString);
        } else {
          // topic
          String route = tp.getPipeline();
          String routeString = convert(route);
          currRouteInfo.putIfAbsent(routeString, new int[3]);
          currRouteInfo.get(routeString)[0]++;
        }
      }
    }
    //LOGGER.info("currRouteInfo: {}", currRouteInfo);
    //LOGGER.info("_routeToCounterMap: {}", _routeToCounterMap);

    for (String routeString : _routeToCounterMap.keySet()) {
      int topicTotalNumber = 0;
      int controllerTotalNumber = 0;
      int workerTotalNumber = 0;
      if (currRouteInfo.containsKey(routeString)) {
        topicTotalNumber = currRouteInfo.get(routeString)[0];
        controllerTotalNumber = currRouteInfo.get(routeString)[1];
        workerTotalNumber = currRouteInfo.get(routeString)[2];
      }
      Counter topicTotalNumberCounter = _routeToCounterMap.get(routeString).get(TOPIC_TOTAL_NUMBER);
      topicTotalNumberCounter.inc(topicTotalNumber - topicTotalNumberCounter.getCount());

      Counter controllerTotalNumberCounter = _routeToCounterMap.get(routeString).get(CONTROLLER_TOTAL_NUMBER);
      controllerTotalNumberCounter.inc(controllerTotalNumber - controllerTotalNumberCounter.getCount());

      Counter workerTotalNumberCounter = _routeToCounterMap.get(routeString).get(WORKER_TOTAL_NUMBER);
      workerTotalNumberCounter.inc(workerTotalNumber - workerTotalNumberCounter.getCount());
      // LOGGER.info("update metrics for {}", routeString);
    }
  }

  public synchronized void updateCurrentStatus() {
    _lock.lock();
    try {
      long currTimeMs = System.currentTimeMillis();
      if (currTimeMs - lastUpdateTimeMs < _conf.getUpdateStatusCoolDownMs()) {
        LOGGER.info("Only {} ms since last updateCurrentStatus, wait for next one", currTimeMs - lastUpdateTimeMs);
        return;
      }
      LOGGER.info("Trying to run controller updateCurrentStatus");

      _workerHelixManager.updateCurrentStatus();

      // Map<InstanceName, InstanceTopicPartitionHolder>
      Map<String, InstanceTopicPartitionHolder> instanceMap = new HashMap<>();
      // Map<TopicName, Map<Pipeline, Instance>>
      Map<String, Map<String, InstanceTopicPartitionHolder>> currTopicToPipelineInstanceMap = new HashMap<>();
      // Map<Pipeline, PriorityQueue<Instance>>
      Map<String, PriorityQueue<InstanceTopicPartitionHolder>> currPipelineToInstanceMap = new HashMap<>();
      // Set<InstanceName>
      List<String> currAvailableControllerList = new ArrayList<>();

      Map<TopicPartition, List<String>> workerRouteToInstanceMap = _workerHelixManager.getWorkerRouteToInstanceMap();
      // Map<Instance, Set<Pipeline>> from IdealState
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap = HelixUtils
          .getInstanceToTopicPartitionsMap(_helixManager, _kafkaValidationManager.getClusterToObserverMap());

      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      currAvailableControllerList.addAll(liveInstances);

      int assignedControllerCount = 0;
      for (String instanceId : instanceToTopicPartitionsMap.keySet()) {
        Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceId);
        // TODO: one instance suppose to have only one route
        for (TopicPartition tp : topicPartitions) {
          String topicName = tp.getTopic();
          if (topicName.startsWith(SEPARATOR)) {
            currPipelineToInstanceMap.putIfAbsent(topicName, new PriorityQueue<>(1,
                InstanceTopicPartitionHolder.totalWorkloadComparator(_pipelineWorkloadMap)));
            InstanceTopicPartitionHolder itph = new InstanceTopicPartitionHolder(instanceId, tp);
            if (workerRouteToInstanceMap.get(tp) != null) {
              itph.addWorkers(workerRouteToInstanceMap.get(tp));
            }
            currPipelineToInstanceMap.get(topicName).add(itph);
            instanceMap.put(instanceId, itph);
            currAvailableControllerList.remove(instanceId);
            assignedControllerCount++;
          }
        }

        for (TopicPartition tp : topicPartitions) {
          String topicName = tp.getTopic();
          if (!topicName.startsWith(SEPARATOR)) {
            if (instanceMap.containsKey(instanceId)) {
              instanceMap.get(instanceId).addTopicPartition(tp);
              currTopicToPipelineInstanceMap.putIfAbsent(topicName, new ConcurrentHashMap<>());
              currTopicToPipelineInstanceMap.get(tp.getTopic()).put(getPipelineFromRoute(tp.getPipeline()),
                  instanceMap.get(instanceId));
            }
          }
        }
      }

      _pipelineToInstanceMap = currPipelineToInstanceMap;
      _topicToPipelineInstanceMap = currTopicToPipelineInstanceMap;
      _availableControllerList = currAvailableControllerList;

      if (_helixManager.isLeader()) {
        _availableController.inc(_availableControllerList.size() - _availableController.getCount());
        _availableWorker.inc(_workerHelixManager.getAvailableWorkerList().size() - _availableWorker.getCount());
        _assignedControllerCount.inc(assignedControllerCount - _assignedControllerCount.getCount());
      }

      // Validation
      validateInstanceToTopicPartitionsMap(instanceToTopicPartitionsMap, instanceMap);

      //LOGGER.info("For controller _pipelineToInstanceMap: {}", _pipelineToInstanceMap);
      //LOGGER.info("For controller _topicToPipelineInstanceMap: {}", _topicToPipelineInstanceMap);
      LOGGER.info("For controller {} available", _availableControllerList.size());

      lastUpdateTimeMs = System.currentTimeMillis();
    } catch (Exception e) {
      LOGGER.error("Got exception in updateCurrentStatus", e);
    } finally {
      _lock.unlock();
    }
  }

  public List<String> extractTopicList(String response) {
    String topicList = response.substring(25, response.length() - 1);
    String[] topics = topicList.split(",");
    List<String> result = new ArrayList<>();
    for (String topic : topics) {
      result.add(topic);
    }
    return result;
  }

  private HostAndPort getHostInfo(String instanceId) throws ControllerException {
    Map<String, HostAndPort> instanceIdAndNameMap = HelixUtils.getInstanceToHostInfoMap(_helixManager);
    HostAndPort hostInfo =  instanceIdAndNameMap.containsKey(instanceId) ? instanceIdAndNameMap.get(instanceId) : null;
    if (hostInfo == null) {
      throw new ControllerException(String.format("Failed to find hostInfo for instanceId %s", instanceId));
    }
    return hostInfo;
  }

  public JSONObject getTopicInfoFromController(String topicName) {
    JSONObject resultJson = new JSONObject();
    Map<String, InstanceTopicPartitionHolder> pipelineToInstanceMap = _topicToPipelineInstanceMap.get(topicName);
    for (String pipeline : pipelineToInstanceMap.keySet()) {
      InstanceTopicPartitionHolder itph = pipelineToInstanceMap.get(pipeline);
      try {
        HostAndPort hostInfo = getHostInfo(itph.getInstanceName());
        String topicResponseBody = HttpClientUtils.getData(_httpClient, _requestConfig,
                hostInfo.getHost(), hostInfo.getPort(), "/topics/" + topicName);
        JSONObject topicsInfoInJson = JSON.parseObject(topicResponseBody);
        resultJson.put(itph.getRouteString(), topicsInfoInJson);
      } catch (Exception e) {
        LOGGER.warn("Failed to curl topic info from controller: {}", itph.getInstanceName(), e);
      }
    }

    return resultJson;
  }

  public IdealState getIdealStateForTopic(String topicName) {
    return _helixAdmin.getResourceIdealState(_helixClusterName, topicName);
  }

  public ExternalView getExternalViewForTopic(String topicName) {
    return _helixAdmin.getResourceExternalView(_helixClusterName, topicName);
  }

  public boolean isPipelineExisted(String pipeline) {
    return _helixAdmin.getResourcesInCluster(_helixClusterName).contains(pipeline);
  }

  public boolean isTopicExisted(String topicName) {
    return _helixAdmin.getResourcesInCluster(_helixClusterName).contains(topicName);
  }

  public boolean isTopicPipelineExisted(String topicName, String pipeline) {
    if (!isPipelineExisted(pipeline) || !isTopicExisted(topicName)) {
      return false;
    }
    for (String partition : getIdealStateForTopic(topicName).getPartitionSet()) {
      if (partition.startsWith(pipeline)) {
        return true;
      }
    }
    return false;
  }

  public List<String> getPipelineLists() {
    List<String> pipelineList = new ArrayList<>();
    for (String resource : _helixAdmin.getResourcesInCluster(_helixClusterName)) {
      if (resource.startsWith(SEPARATOR)) {
        pipelineList.add(resource);
      }
    }
    return pipelineList;
  }

  public List<String> getTopicLists() {
    List<String> toplicList = new ArrayList<>();
    for (String resource : _helixAdmin.getResourcesInCluster(_helixClusterName)) {
      if (!resource.startsWith(SEPARATOR)) {
        toplicList.add(resource);
      }
    }
    return toplicList;
  }

  public Map<String, Map<String, InstanceTopicPartitionHolder>> getTopicToPipelineInstanceMap() {
    return _topicToPipelineInstanceMap;
  }

  public Map<String, PriorityQueue<InstanceTopicPartitionHolder>> getPipelineToInstanceMap() {
    return _pipelineToInstanceMap;
  }

  public Map<String, InstanceTopicPartitionHolder> getTopic(String topicName) {
    return _topicToPipelineInstanceMap.get(topicName);
  }

  public synchronized void handleLiveInstanceChange(boolean onlyCheckOffline, boolean forceBalance) throws Exception {
    _lock.lock();
    try {
      LOGGER.info("handleLiveInstanceChange() wake up!");

      // Check if any controller in route is down
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap = HelixUtils
          .getInstanceToTopicPartitionsMap(_helixManager, _kafkaValidationManager.getClusterToObserverMap());
      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      List<String> instanceToReplace = new ArrayList<>();
      boolean routeControllerDown = false;
      // Check if any worker in route is down
      boolean routeWorkerDown = false;
      if (_enableRebalance || forceBalance) {
        for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
          if (!liveInstances.contains(instanceName)) {
            routeControllerDown = true;
            instanceToReplace.add(instanceName);
          }
        }

        LOGGER.info("Controller need to replace: {}", instanceToReplace);
        // Make sure controller status is up-to-date
        updateCurrentStatus();
        // Happy scenario: instance contains route topic
        for (String instance : instanceToReplace) {
          Set<TopicPartition> tpOrRouteSet = instanceToTopicPartitionsMap.get(instance);
          for (TopicPartition tpOrRoute : tpOrRouteSet) {
            if (tpOrRoute.getTopic().startsWith(SEPARATOR)) {
              String pipeline = tpOrRoute.getTopic();
              int routeId = tpOrRoute.getPartition();
              if(_availableControllerList.isEmpty()){
                LOGGER.warn("no available controller to process the route {}@{}", pipeline, routeId);
                break;
              }
              String newInstanceName = _availableControllerList.remove(0);
              LOGGER.info("Controller {} in route {}@{} will be replaced by {}", instance, pipeline, routeId,
                  newInstanceName);
              InstanceTopicPartitionHolder newInstance = new InstanceTopicPartitionHolder(newInstanceName, tpOrRoute);

              List<TopicPartition> tpToReassign = new ArrayList<>();
              PriorityQueue<InstanceTopicPartitionHolder> itphList = _pipelineToInstanceMap.get(pipeline);
              for (InstanceTopicPartitionHolder itph : itphList) {
                if (itph.getInstanceName().equals(instance)) {
                  tpToReassign.addAll(itph.getServingTopicPartitionSet());
                  // TODO: is it possible to have different route on same host?
                  break;
                }
              }

              // Helix doesn't guarantee the order of execution, so we have to wait for new controller to be online
              // before reassigning topics
              // But this might cause long rebalance time
              _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
                  IdealStateBuilder
                      .resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                          pipeline, String.valueOf(routeId), newInstanceName));

              long ts1 = System.currentTimeMillis();
              while (!isControllerOnline(newInstanceName, pipeline, String.valueOf(routeId))) {
                if (System.currentTimeMillis() - ts1 > 30000) {
                  break;
                }
                try {
                  // Based on testing, the wait time is usually in the order of 100 ms
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }

              long ts2 = System.currentTimeMillis();
              LOGGER.info("Controller {} in route {}@{} is replaced by {}, it took {} ms",
                  instance, pipeline, routeId, newInstanceName, ts2 - ts1);

              for (TopicPartition tp : tpToReassign) {
                _helixAdmin.setResourceIdealState(_helixClusterName, tp.getTopic(),
                    IdealStateBuilder
                        .resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, tp.getTopic()),
                            tp.getTopic(), pipeline + SEPARATOR + routeId, newInstanceName));
              }

              LOGGER.info("Controller {} in route {}@{} is replaced by {}, topics are reassigned, it took {} ms",
                  instance, pipeline, routeId, newInstanceName, System.currentTimeMillis() - ts2);
              break;
            }
          }
        }
        // Failure scenario: instance doesn't contain route topic
        // e.g. route and the topic in that route are not assigned to the same host
        // In this case, assume the instance of the route is correct and reassign the topic to that host
        for (String instance : instanceToTopicPartitionsMap.keySet()) {
          Set<TopicPartition> topicPartitionSet = instanceToTopicPartitionsMap.get(instance);
          if (topicPartitionSet.isEmpty()) {
            continue;
          }
          boolean foundRoute = false;
          for (TopicPartition tp : topicPartitionSet) {
            if (tp.getTopic().startsWith(SEPARATOR)) {
              foundRoute = true;
              break;
            }
          }
          if (!foundRoute) {
            routeControllerDown = true;
            String instanceForRoute = null;
            // Find the host for its route
            String route = topicPartitionSet.iterator().next().getPipeline();
            for (String pipeline : _pipelineToInstanceMap.keySet()) {
              if (pipeline.equals(getPipelineFromRoute(route))) {
                for (InstanceTopicPartitionHolder itph : _pipelineToInstanceMap.get(pipeline)) {
                  if (itph.getRouteString().equals(route)) {
                    instanceForRoute = itph.getInstanceName();
                    break;
                  }
                }
              }
            }

            LOGGER.info("Need to reassign: {} from {} to {}", topicPartitionSet, instance, instanceForRoute);
            for (TopicPartition tp : topicPartitionSet) {
              _helixAdmin.setResourceIdealState(_helixClusterName, tp.getTopic(),
                  IdealStateBuilder
                      .resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, tp.getTopic()),
                          tp.getTopic(), route, instanceForRoute));
            }
          }
        }

        if (routeControllerDown) {
          updateCurrentStatus();
        }

        HelixManager workeManager = _workerHelixManager.getHelixManager();
        Map<String, Set<TopicPartition>> workerInstanceToTopicPartitionsMap = HelixUtils
            .getInstanceToTopicPartitionsMap(workeManager, null);
        List<String> workerLiveInstances = HelixUtils.liveInstances(workeManager);
        Map<String, List<String>> workerPipelineToRouteIdToReplace = new HashMap<>();
        List<String> workerToReplace = new ArrayList<>();

        for (String instanceName : workerInstanceToTopicPartitionsMap.keySet()) {
          if (!workerLiveInstances.contains(instanceName)) {
            routeWorkerDown = true;
            TopicPartition route = workerInstanceToTopicPartitionsMap.get(instanceName).iterator().next();
            workerPipelineToRouteIdToReplace.putIfAbsent(route.getTopic(), new ArrayList<>());
            workerPipelineToRouteIdToReplace.get(route.getTopic()).add(String.valueOf(route.getPartition()));
            workerToReplace.add(instanceName);
            LOGGER.info("Worker changed: {} for {}", instanceName, route);
          }
        }
        if (!routeWorkerDown) {
          LOGGER.info("No worker in route is changed, do nothing!");
        } else {
          LOGGER.info("Worker need to replace: {}, {}", workerToReplace, workerPipelineToRouteIdToReplace);
          // Make sure worker status is up-to-date
          if (!routeControllerDown) {
            updateCurrentStatus();
          }
          _workerHelixManager.replaceWorkerInMirrorMaker(workerPipelineToRouteIdToReplace, workerToReplace);

          updateCurrentStatus();
        }
      } else {
        LOGGER.info("AutoBalancing is disabled, do nothing");
      }

      if (onlyCheckOffline) {
        return;
      }

      LOGGER.info("Start rebalancing current cluster");
      // Haven't run updateCurrentStatus() before
      if (!routeControllerDown && !routeWorkerDown) {
        updateCurrentStatus();
      }

      if (_enableAutoScaling) {
        scaleCurrentCluster();
      } else {
        LOGGER.info("AutoScaling is disabled, do nothing");
      }

    } finally {
      _lock.unlock();
    }
  }

  private boolean isControllerOnline(String instance, String routeName, String routeId) {
    LOGGER.info("Check if {} is online for {}, {}", instance, routeName, routeId);
    try {
      String[] srcDst = routeName.split(SEPARATOR);
      String controllerWorkerHelixClusterName = "/controller-worker-" + srcDst[1] + "-" + srcDst[2] + "-" + routeId;
      String leaderPath = controllerWorkerHelixClusterName + "/CONTROLLER/LEADER";
      if(!_zkClient.exists(leaderPath)){
        LOGGER.info("leaderPath : {} not existed", leaderPath);
        return false;
      }
      JSONObject json = JSON.parseObject(_zkClient.readData(leaderPath).toString());
      String currLeader = String.valueOf(json.get("id"));
      LOGGER.info("current leader is {}, expect {}", currLeader, instance);
      return currLeader.equals(instance);
    } catch (Exception e) {
      LOGGER.error("Got error when checking current leader", e);
      return false;
    }
  }

  public void scaleCurrentCluster() throws Exception {
    int oldTotalNumWorker = 0;
    int newTotalNumWorker = 0;
    Map<String, Integer> _routeWorkerOverrides = getRouteWorkerOverride();
    for (String pipeline : _pipelineToInstanceMap.keySet()) {
      LOGGER.info("Start rescale pipeline: {}", pipeline);
      PriorityQueue<InstanceTopicPartitionHolder> newItphQueue = new PriorityQueue<>(1,
          InstanceTopicPartitionHolder.totalWorkloadComparator(_pipelineWorkloadMap));
      // TODO: what if routeId is not continuous
      int nextRouteId = _pipelineToInstanceMap.get(pipeline).size();
      for (InstanceTopicPartitionHolder itph : _pipelineToInstanceMap.get(pipeline)) {
        if (itph.getTotalNumPartitions() > _maxNumPartitionsPerRoute) {
          LOGGER.info("Checking route {} with controller {} and topics {} since it exceeds maxNumPartitionsPerRoute {}",
              itph.getRouteString(), itph.getInstanceName(), itph.getServingTopicPartitionSet(),
              _maxNumPartitionsPerRoute);
          while (itph.getTotalNumPartitions() > _maxNumPartitionsPerRoute) {
            // Only one topic left, do nothing
            if (itph.getNumServingTopicPartitions() == 1) {
              LOGGER.info("Only one topic {} in route {}, do nothing",
                  itph.getServingTopicPartitionSet().iterator().next(), itph.getRouteString());
              break;
            }

            // Get the topic with largest number of partitions
            TopicPartition tpToMove = new TopicPartition("tmp", -1);
            for (TopicPartition tp : itph.getServingTopicPartitionSet()) {
              if (tp.getPartition() > tpToMove.getPartition()) {
                tpToMove = tp;
              }
            }

            // If existing lightest route cannot fit the largest topic to move
            if (newItphQueue.isEmpty() ||
                newItphQueue.peek().getTotalNumPartitions() + tpToMove.getPartition() > _initMaxNumPartitionsPerRoute) {
              try {
                InstanceTopicPartitionHolder newHolder = createNewRoute(pipeline, nextRouteId);

                _helixAdmin.setResourceIdealState(_helixClusterName, tpToMove.getTopic(),
                    IdealStateBuilder.resetCustomIdealStateFor(
                        _helixAdmin.getResourceIdealState(_helixClusterName, tpToMove.getTopic()),
                        tpToMove.getTopic(), itph.getRouteString(), newHolder.getRouteString(),
                        newHolder.getInstanceName()));

                itph.removeTopicPartition(tpToMove);
                newHolder.addTopicPartition(tpToMove);
                newItphQueue.add(newHolder);
                nextRouteId++;

              } catch (Exception e) {
                LOGGER.error("Got exception when create a new route when rebalancing, abandon!", e);
                throw new Exception("Got exception when create a new route when rebalancing, abandon!", e);
              }
            } else {
              InstanceTopicPartitionHolder newHolder = newItphQueue.poll();

              _helixAdmin.setResourceIdealState(_helixClusterName, tpToMove.getTopic(),
                  IdealStateBuilder.resetCustomIdealStateFor(
                      _helixAdmin.getResourceIdealState(_helixClusterName, tpToMove.getTopic()),
                      tpToMove.getTopic(), itph.getRouteString(), newHolder.getRouteString(),
                      newHolder.getInstanceName()));
              itph.removeTopicPartition(tpToMove);
              newHolder.addTopicPartition(tpToMove);
              newItphQueue.add(newHolder);
            }
          }
        }
        newItphQueue.add(itph);
      }

      // After moving topics, scale workers based on workload
      int rescaleFailedCount = 0;
      for (InstanceTopicPartitionHolder itph : newItphQueue) {
        oldTotalNumWorker += itph.getWorkerSet().size();
        String routeString = itph.getRouteString();
        int initWorkerCount = _initMaxNumWorkersPerRoute;
        if (_routeWorkerOverrides.containsKey(routeString) && _routeWorkerOverrides.get(routeString) > initWorkerCount) {
          initWorkerCount = _routeWorkerOverrides.get(routeString);
        }

        try {
          HostAndPort hostInfo = getHostInfo(itph.getInstanceName());
          String result = HttpClientUtils.getData(_httpClient, _requestConfig,
                  hostInfo.getHost(), hostInfo.getPort(), "/admin/workloadinfo");
          ControllerWorkloadInfo workloadInfo = JSONObject.parseObject(result, ControllerWorkloadInfo.class);
          TopicWorkload totalWorkload = workloadInfo.getTopicWorkload();

          if (workloadInfo != null && workloadInfo.getNumOfExpectedWorkers() != 0) {
            _pipelineWorkloadMap.put(itph.getRouteString(), totalWorkload);
            int expectedNumWorkers = workloadInfo.getNumOfExpectedWorkers();
            LOGGER.info("Current {} workers in route {}, expect {} workers",
                itph.getWorkerSet().size(), itph.getRouteString(), expectedNumWorkers);
            int actualExpectedNumWorkers = getActualExpectedNumWorkers(expectedNumWorkers, initWorkerCount);
            LOGGER.info("Current {} workers in route {}, actual expect {} workers",
                itph.getWorkerSet().size(), itph.getRouteString(), actualExpectedNumWorkers);

            if (actualExpectedNumWorkers > itph.getWorkerSet().size()) {
              LOGGER.info("Current {} workers in route {}, actual expect {} workers, add {} workers",
                  itph.getWorkerSet().size(), itph.getRouteString(), actualExpectedNumWorkers, actualExpectedNumWorkers - itph.getWorkerSet().size());
              // TODO: handle exception
              _workerHelixManager.addWorkersToMirrorMaker(itph, itph.getRoute().getTopic(),
                  itph.getRoute().getPartition(), actualExpectedNumWorkers - itph.getWorkerSet().size());
            }

            if (actualExpectedNumWorkers < itph.getWorkerSet().size()) {
              LOGGER.info("Current {} workers in route {}, actual expect {} workers, remove {} workers",
                  itph.getWorkerSet().size(), itph.getRouteString(), actualExpectedNumWorkers, itph.getWorkerSet().size() - actualExpectedNumWorkers);
              // TODO: handle exception
              _workerHelixManager.removeWorkersToMirrorMaker(itph, itph.getRoute().getTopic(),
                  itph.getRoute().getPartition(), _numOfWorkersBatchSize);
            }
            newTotalNumWorker += actualExpectedNumWorkers;
          } else {
            LOGGER.warn("Get workload on {} for route: {} returns 0. No change on number of workers", hostInfo, itph.getRouteString());
            newTotalNumWorker += itph.getWorkerSet().size();
            rescaleFailedCount ++;
          }
        } catch (Exception e) {
          rescaleFailedCount ++;
          LOGGER.error(String.format("Get workload error when connecting to %s for route %s. No change on number of workers", itph.getInstanceName(), itph.getRouteString()), e);
          newTotalNumWorker += itph.getWorkerSet().size();
          rescaleFailedCount ++;
        }
      }
      _pipelineToInstanceMap.put(pipeline, newItphQueue);
      _rescaleFailedCount.inc(rescaleFailedCount - _rescaleFailedCount.getCount());
    }
    LOGGER.info("oldTotalNumWorker: {}, newTotalNumWorker: {}", oldTotalNumWorker, newTotalNumWorker);
  }

  private int getActualExpectedNumWorkers(int expectedNumWorkers, int initWorkerPerRoute) {
    if (expectedNumWorkers <= initWorkerPerRoute) {
      return initWorkerPerRoute;
    }
    if (expectedNumWorkers >= _maxNumWorkersPerRoute) {
      return _maxNumWorkersPerRoute;
    }
    return (int) (Math.ceil((double) (expectedNumWorkers - initWorkerPerRoute) / _numOfWorkersBatchSize) * _numOfWorkersBatchSize) + initWorkerPerRoute;
  }

  public int getExpectedNumWorkers(int currNumPartitions) {
    return Math.min(_maxNumWorkersPerRoute, _initMaxNumWorkersPerRoute +
        (_maxNumWorkersPerRoute - _initMaxNumWorkersPerRoute) *
            (currNumPartitions - _initMaxNumPartitionsPerRoute) /
            (_maxNumPartitionsPerRoute - _initMaxNumPartitionsPerRoute));
  }

  public InstanceTopicPartitionHolder createNewRoute(String pipeline, int routeId) throws Exception {
    if (_availableControllerList.isEmpty()) {
      LOGGER.info("No available controller!");
      throw new Exception("No available controller!");
    }

    if (_workerHelixManager.getAvailableWorkerList().isEmpty()) {
      LOGGER.info("No available worker!");
      throw new Exception("No available worker!");
    }

    String instanceName = _availableControllerList.get(0);
    InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName,
        new TopicPartition(pipeline, routeId));
    if (!isPipelineExisted(pipeline)) {
      setEmptyResourceConfig(pipeline);
      _helixAdmin.addResource(_helixClusterName, pipeline,
          IdealStateBuilder.buildCustomIdealStateFor(pipeline, String.valueOf(routeId), instance));
    } else {
      LOGGER.info("Expanding pipeline {} new partition {} to instance {}", pipeline, routeId, instance);
      _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
          IdealStateBuilder.expandCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
              pipeline, String.valueOf(routeId), instance));
      LOGGER.info("New IdealState: {}", _helixAdmin.getResourceIdealState(_helixClusterName, pipeline));
    }

    String[] srcDst = pipeline.split(SEPARATOR);
    String controllerWorkerHelixClusterName = "controller-worker-" + srcDst[1] + "-" + srcDst[2] + "-" + routeId;
    HelixManager spectator = HelixManagerFactory.getZKHelixManager(controllerWorkerHelixClusterName,
        _instanceId,
        InstanceType.SPECTATOR,
        _helixZkURL);

    long ts1 = System.currentTimeMillis();
    while (true) {
      try {
        spectator.connect();
        break;
      } catch (Exception e) {
        // Do nothing
      }

      if (System.currentTimeMillis() - ts1 > 60000) {
        throw new Exception(String.format("Controller %s failed to set up new route cluster %s!",
            instanceName, controllerWorkerHelixClusterName));
      }
      Thread.sleep(1000);
    }

    _availableControllerList.remove(instanceName);
    _pipelineToInstanceMap.put(pipeline, new PriorityQueue<>(1,
        InstanceTopicPartitionHolder.totalWorkloadComparator(_pipelineWorkloadMap)));
    _pipelineToInstanceMap.get(pipeline).add(instance);
    _assignedControllerCount.inc();
    _workerHelixManager.addTopicToMirrorMaker(instance, pipeline, routeId);

    // register metrics
    String routeString = srcDst[1] + "-" + srcDst[2] + "-" + routeId;
    maybeRegisterMetrics(routeString);

    spectator.disconnect();
    return instance;
  }

  public InstanceTopicPartitionHolder maybeCreateNewRoute(
      PriorityQueue<InstanceTopicPartitionHolder> instanceList,
      String topicName,
      int numPartitions,
      String pipeline) throws Exception {

    LOGGER.info("maybeCreateNewRoute, topicName: {}, numPartitions: {}, pipeline: {}", topicName, numPartitions,
        pipeline);

    Set<Integer> routeIdSet = new HashSet<>();
    for (InstanceTopicPartitionHolder instance : instanceList) {
      if (instance.getTotalNumPartitions() + numPartitions < _initMaxNumPartitionsPerRoute) {
        return instance;
      }
      routeIdSet.add(instance.getRoute().getPartition());
    }

    // For now we don't delete route even it's empty. so routeId should be 0,1...N
    int routeId = 0;
    while (routeId < routeIdSet.size() + 1) {
      if (!routeIdSet.contains(routeId)) {
        break;
      }
      routeId++;
    }

    return createNewRoute(pipeline, routeId);
  }

  public synchronized void addTopicToMirrorMaker(
      String topicName,
      int numPartitions,
      String src,
      String dst,
      String pipeline) throws Exception {
    _lock.lock();
    try {
      LOGGER.info("Trying to add topic: {} to pipeline: {}", topicName, pipeline);

      if (!isPipelineExisted(pipeline)) {
        createNewRoute(pipeline, 0);
      } else {
        LOGGER.info("Pipeline already existed!");
      }

      boolean isSameDc = src.substring(0, 3).equals(dst.substring(0, 3));

      InstanceTopicPartitionHolder instance = maybeCreateNewRoute(_pipelineToInstanceMap.get(pipeline), topicName,
          numPartitions, pipeline);
      String route = instance.getRouteString();
      if (!isTopicExisted(topicName)) {
        setEmptyResourceConfig(topicName);
        _helixAdmin.addResource(_helixClusterName, topicName,
            IdealStateBuilder.buildCustomIdealStateFor(topicName, route, instance));
      } else {
        _helixAdmin.setResourceIdealState(_helixClusterName, topicName,
            IdealStateBuilder.expandCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, topicName),
                topicName, route, instance));
      }

      instance.addTopicPartition(new TopicPartition(topicName, numPartitions, pipeline));
      _topicToPipelineInstanceMap.putIfAbsent(topicName, new ConcurrentHashMap<>());
      _topicToPipelineInstanceMap.get(topicName).put(pipeline, instance);
    } finally {
      _lock.unlock();
    }
  }

  // TODO: fix status if accidentally expanding a topic to a larger number
  public synchronized void expandTopicInMirrorMaker(
      String topicName,
      String srcCluster,
      String pipeline,
      int newNumPartitions) throws Exception {
    _lock.lock();
    try {
      LOGGER.info("Trying to expand topic: {} in pipeline: {} to {} partitions", topicName, pipeline, newNumPartitions);

      if (!isTopicPipelineExisted(topicName, pipeline)) {
        updateCurrentStatus();
      }
      if (!isTopicPipelineExisted(topicName, pipeline)) {
        LOGGER.info("Topic {} doesn't exist in pipeline {}, abandon expanding topic", topicName, pipeline);
        throw new Exception(String.format("Topic %s doesn't exist in pipeline %s, abandon expanding topic!",
            topicName, pipeline));
      }

      InstanceTopicPartitionHolder itph = _topicToPipelineInstanceMap.get(topicName).get(pipeline);

      boolean found = false;
      int oldNumPartitions = 0;
      for (TopicPartition tp : itph.getServingTopicPartitionSet()) {
        if (tp.getTopic().equals(topicName)) {
          found = true;
          oldNumPartitions = tp.getPartition();
          if (newNumPartitions <= oldNumPartitions) {
            LOGGER.info("New partition {} is not bigger than current partition {} of topic {}, abandon expanding topic",
                newNumPartitions, oldNumPartitions, topicName);
            throw new Exception(String.format("New partition %s is not bigger than current partition %s of topic %s, "
                + "abandon expanding topic!", newNumPartitions, oldNumPartitions, topicName));
          }
        }
      }

      if (!found) {
        LOGGER.info("Failed to find topic {} in pipeline {}, abandon expanding topic", topicName, pipeline);
        throw new Exception(String.format("Failed to find topic %s in pipeline %s, abandon expanding topic!",
            topicName, pipeline));
      }

      JSONObject entity = new JSONObject();
      entity.put("topic", topicName);
      entity.put("numPartitions", newNumPartitions);
      HostAndPort hostInfo = getHostInfo(itph.getInstanceName());
      int respCode = HttpClientUtils.putData(_httpClient, _requestConfig,
              hostInfo.getHost(), hostInfo.getPort(), "/topics", entity);
      if (respCode != 200) {
        LOGGER.info("Got error from controller {} when expanding topic {} with respCode {}",
            itph.getInstanceName(), topicName, respCode);
        throw new Exception(String.format("Got error from controller %s when expanding topic %s with respCode %s",
            itph.getInstanceName(), topicName, respCode));
      }

      itph.removeTopicPartition(new TopicPartition(topicName, oldNumPartitions, pipeline));
      itph.addTopicPartition(new TopicPartition(topicName, newNumPartitions, pipeline));
      _kafkaValidationManager.getClusterToObserverMap().get(srcCluster).tryUpdateTopic(topicName);
    } finally {
      _lock.unlock();
    }
  }

  public synchronized void deletePipelineInMirrorMaker(String pipeline) {
    // TODO: delete topic first
    _lock.lock();
    try {
      LOGGER.info("Trying to delete pipeline: {}", pipeline);
      PriorityQueue<InstanceTopicPartitionHolder> itphSet = _pipelineToInstanceMap.get(pipeline);
      for (InstanceTopicPartitionHolder itph : itphSet) {
        if (itph.getTotalNumPartitions() != 0) {
          throw new UnsupportedOperationException("Delete non-empty pipeline is not allowed, serving number of partitions: "
              + String.valueOf(itph.getTotalNumPartitions()));
        }
      }


      _workerHelixManager.deletePipelineInMirrorMaker(pipeline);
      _helixAdmin.dropResource(_helixClusterName, pipeline);

      _pipelineToInstanceMap.remove(pipeline);
      // Maybe clear instanceHolder's worker set
      List<String> topicsToDelete = new ArrayList<>();
      for (String topic : _topicToPipelineInstanceMap.keySet()) {
        if (_topicToPipelineInstanceMap.get(topic).containsKey(pipeline)) {
          _topicToPipelineInstanceMap.get(topic).remove(pipeline);
        }
        if (_topicToPipelineInstanceMap.get(topic).isEmpty()) {
          topicsToDelete.add(topic);
        }
      }
      for (String topic : topicsToDelete) {
        _topicToPipelineInstanceMap.remove(topic);
      }
    } finally {
      _lock.unlock();
    }
  }

  public synchronized void deleteTopicInMirrorMaker(String topicName, String src, String dst, String pipeline)
      throws Exception {
    _lock.lock();
    try {
      LOGGER.info("Trying to delete topic: {} in pipeline: {}", topicName, pipeline);

      InstanceTopicPartitionHolder instance = _topicToPipelineInstanceMap.get(topicName).get(pipeline);
      IdealState currIdealState = getIdealStateForTopic(topicName);
      if (currIdealState.getPartitionSet().contains(instance.getRouteString())
          && currIdealState.getNumPartitions() == 1) {
        _helixAdmin.dropResource(_helixClusterName, topicName);
      } else {
        _helixAdmin.setResourceIdealState(_helixClusterName, topicName,
            IdealStateBuilder.shrinkCustomIdealStateFor(currIdealState, topicName, instance.getRouteString()));
      }
      TopicPartition tp = _kafkaValidationManager.getClusterToObserverMap().get(src)
          .getTopicPartitionWithRefresh(topicName);
      instance.removeTopicPartition(tp);
      _topicToPipelineInstanceMap.get(topicName).remove(pipeline);
      if (_topicToPipelineInstanceMap.get(topicName).keySet().size() == 0) {
        _topicToPipelineInstanceMap.remove(topicName);
      }
      if (instance.getServingTopicPartitionSet().isEmpty()) {
        _availableControllerList.add(instance.getInstanceName());
        _assignedControllerCount.dec();
      }
    } finally {
      _lock.unlock();
    }
  }

  private synchronized void setEmptyResourceConfig(String topicName) {
    _helixAdmin.setConfig(new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE)
        .forCluster(_helixClusterName)
        .forResource(topicName).build(), new HashMap<>());
  }

  public KafkaClusterValidationManager getKafkaValidationManager() {
    return _kafkaValidationManager;
  }

  public WorkerHelixManager getWorkerHelixManager() {
    return _workerHelixManager;
  }

  public IuReplicatorConf getConf() {
    return _conf;
  }

  private static String getPipelineFromRoute(String route) {
    return route.substring(0, route.lastIndexOf("@"));
  }

  private static String getSrc(String pipeline) {
    String[] srcDst = pipeline.split(SEPARATOR);
    return srcDst[1];
  }

  public void disableAutoScaling() {
    _enableAutoScaling = false;
  }

  public void enableAutoScaling() {
    _enableAutoScaling = true;
  }

  public boolean isAutoScalingEnabled() {
    return _enableAutoScaling;
  }

  public void disableAutoBalancing() {
    _enableRebalance = false;
  }

  public void enableAutoBalancing() {
    _enableRebalance = true;
  }

  public boolean isAutoBalancingEnabled() {
    return _enableRebalance;
  }

  public boolean getControllerAutobalancingStatus(String controllerInstance) throws ControllerException {
    try {
      HostAndPort hostInfo = getHostInfo(controllerInstance);
      String result = HttpClientUtils
          .getData(_httpClient, _requestConfig, hostInfo.getHost(), hostInfo.getPort(),
              "/admin/" + "autobalancing_status");
      return result.equalsIgnoreCase("enabled");
    } catch (IOException | URISyntaxException ex) {
      String msg = String.format("Got error from controller %s when trying to get balancing status",
          controllerInstance);
      LOGGER.error(msg, ex);
      throw new ControllerException(msg, ex);
    }
  }

  /**
   * RPC call to notify controller to change autobalancing status.
   * No retry
   *
   * @param controllerInstance The controller InstanceName
   * @param enable             whether to enable autobalancing
   * @return
   */
  public boolean notifyControllerAutobalancing(String controllerInstance, boolean enable) throws ControllerException {
    String cmd = enable ? "enable_autobalancing" : "disable_autobalancing";
    try {
      HostAndPort hostInfo = getHostInfo(controllerInstance);
      String result = HttpClientUtils
          .postData(_httpClient, _requestConfig, hostInfo.getHost(), hostInfo.getPort(),
              "/admin/" + cmd);
      JSONObject resultJson = JSON.parseObject(result);
      return resultJson.getBoolean("auto_balancing") == enable;
    } catch (IOException | URISyntaxException ex) {
      String msg = String.format("Got error from controller %s when trying to do %s",
          controllerInstance, cmd);
      LOGGER.error(msg, ex);
      throw new ControllerException(msg, ex);
    }
  }

  public Map<String, Integer> getRouteWorkerOverride() {
    List<ZNRecord> znRecordList = _helixPropertyStore.getChildren(PIPELINE_PATH, null, AccessOption.PERSISTENT);
    Map<String, Integer> hashMap = new HashMap<>();
    if (znRecordList == null) {
      _helixPropertyStore.create(PIPELINE_PATH, new ZNRecord(""), AccessOption.PERSISTENT);
      return hashMap;
    }
    for (ZNRecord znRecord : znRecordList) {
      hashMap.put(znRecord.getId(), znRecord.getIntField(WORKER_NUMBER_OVERRIDE, _initMaxNumWorkersPerRoute));
    }
    return hashMap;
  }

  public void updateRouteWorkerOverride(String pipeline, Integer value) {
    String resourcePath = PIPELINE_PATH + "/" + pipeline;
    ZNRecord znRecord = _helixPropertyStore.get(resourcePath, null, AccessOption.PERSISTENT);
    if (znRecord == null) {
      znRecord = new ZNRecord(pipeline);
    }
    znRecord.setIntField(WORKER_NUMBER_OVERRIDE, value);
    _helixPropertyStore.set(resourcePath, znRecord, AccessOption.PERSISTENT);
  }
}
