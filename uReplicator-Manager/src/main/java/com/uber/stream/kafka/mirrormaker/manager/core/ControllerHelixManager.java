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
package com.uber.stream.kafka.mirrormaker.manager.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Counter;
import com.uber.stream.kafka.mirrormaker.common.configuration.IuReplicatorConf;
import com.uber.stream.kafka.mirrormaker.common.core.IHelixManager;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.core.WorkloadInfoRetriever;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HttpClientUtils;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.reporter.HelixKafkaMirrorMakerMetricsReporter;
import com.uber.stream.kafka.mirrormaker.manager.validation.SourceKafkaClusterValidationManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
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
  private final SourceKafkaClusterValidationManager _srcKafkaValidationManager;
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final WorkerHelixManager _workerHelixManager;
  private LiveInstanceChangeListener _liveInstanceChangeListener;
  private Map<String, WorkloadInfoRetriever> _workloadInfoRetrieverMap;

  private final CloseableHttpClient _httpClient;
  private final int _controllerPort;
  private final RequestConfig _requestConfig;

  private final int _workloadRefreshPeriodInSeconds;
  private final int _initMaxNumPartitionsPerRoute;
  private final double _initMaxWorkloadPerWorkerByteDc;
  private final double _initMaxWorkloadPerWorkerByteXdc;
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

  private static final Counter _availableController = new Counter();
  private static final Counter _availableWorker = new Counter();
  private static final Counter _nonParityTopic = new Counter();
  private static final Counter _validateWrongCount = new Counter();

  private ReentrantLock _lock = new ReentrantLock();
  private Map<String, Map<String, InstanceTopicPartitionHolder>> _topicToPipelineInstanceMap;
  private Map<String, PriorityQueue<InstanceTopicPartitionHolder>> _pipelineToInstanceMap;
  private List<String> _availableControllerList;

  private long lastUpdateTimeMs = 0L;

  private ZkClient _zkClient;

  public ControllerHelixManager(SourceKafkaClusterValidationManager srcKafkaValidationManager,
      ManagerConf managerConf) {
    _conf = managerConf;
    _srcKafkaValidationManager = srcKafkaValidationManager;
    _initMaxNumPartitionsPerRoute = managerConf.getInitMaxNumPartitionsPerRoute();
    _maxNumPartitionsPerRoute = managerConf.getMaxNumPartitionsPerRoute();
    _initMaxWorkloadPerWorkerByteDc = managerConf.getInitMaxWorkloadPerWorkerByteDc();
    _initMaxWorkloadPerWorkerByteXdc = managerConf.getInitMaxWorkloadPerWorkerByteXdc();
    _initMaxNumWorkersPerRoute = managerConf.getInitMaxNumWorkersPerRoute();
    _maxNumWorkersPerRoute = managerConf.getMaxNumWorkersPerRoute();
    _workloadRefreshPeriodInSeconds = managerConf.getWorkloadRefreshPeriodInSeconds();
    _workerHelixManager = new WorkerHelixManager(managerConf);
    _workloadInfoRetrieverMap = new ConcurrentHashMap<>();
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
    _controllerPort = managerConf.getControllerPort();
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

    initWorkloadInfoRetriever();

    updateCurrentStatus();

    LOGGER.info("Trying to register ControllerLiveInstanceChangeListener");
    _liveInstanceChangeListener = new ControllerLiveInstanceChangeListener(this, _helixManager, _workloadRefreshPeriodInSeconds);
    try {
      _helixManager.addLiveInstanceChangeListener(_liveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add ControllerLiveInstanceChangeListener");
    }
  }

  private void initWorkloadInfoRetriever() {
    for (String cluster : _conf.getSourceClusters()) {
      String srcKafkaZkPath = (String) _conf.getProperty(CONFIG_KAFKA_CLUSTER_KEY_PREFIX + cluster);
      _workloadInfoRetrieverMap.put(cluster, new WorkloadInfoRetriever(this, srcKafkaZkPath));
      _workloadInfoRetrieverMap.get(cluster).start();
    }
  }

  public synchronized void stop() throws IOException {
    LOGGER.info("Trying to stop ManagerControllerHelix!");
    for (WorkloadInfoRetriever retriever : _workloadInfoRetrieverMap.values()) {
      retriever.stop();
    }
    _zkClient.close();
    _workerHelixManager.stop();
    _helixManager.disconnect();
    _httpClient.close();
  }

  private void registerMetrics() {
    try {
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("controller.available.counter",
          _availableController);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("worker.available.counter",
          _availableWorker);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("topic.non-parity.counter",
          _nonParityTopic);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("validate.wrong.counter",
          _validateWrongCount);
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
        HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(route + ".topic.totalNumber",
            _routeToCounterMap.get(route).get(TOPIC_TOTAL_NUMBER));
        //HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(routeString + ".topic.errorNumber",
        //    _routeToCounterMap.get(routeString).get(TOPIC_ERROR_NUMBER));
        HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(route + ".controller.totalNumber",
            _routeToCounterMap.get(route).get(CONTROLLER_TOTAL_NUMBER));
        //HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(routeString + "controller.errorNumber",
        //    _routeToCounterMap.get(routeString).get(CONTROLLER_ERROR_NUMBER));
        HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(route + ".worker.totalNumber",
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

  private void validateInstanceToTopicPartitionsMap(Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap,
      Map<String, InstanceTopicPartitionHolder> instanceMap) {
    LOGGER.info("\n\nFor controller instanceToTopicPartitionsMap:");
    int validateWrongCount = 0;
    for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
      Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
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
        LOGGER.error("Validate WRONG: Incorrect route found for InstanceName: {}, route: {}, pipelines: {}, #workers: {}, worker: {}",
            instanceName, routeSet, topicRouteSet, instanceMap.get(instanceName).getWorkerSet().size(), instanceMap.get(instanceName).getWorkerSet());
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
        if (mismatchTopicPartition.isEmpty()) {
          LOGGER.info("Validate OK: InstanceName: {}, route: {}, #topics: {}, #partitions: {}, #workers: {}, worker: {}", instanceName, routeSet,
              topicPartitions.size() - 1, partitionCount, instanceMap.get(instanceName).getWorkerSet().size(), instanceMap.get(instanceName).getWorkerSet());

          try {
            // try find topic mismatch between manager and controller
            String topicResult = HttpClientUtils.getData(_httpClient, _requestConfig,
                instanceName, _controllerPort, "/topics");
            LOGGER.debug("Get topics from {}: {}", instanceName, topicResult);
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
              LOGGER.info("Validate WRONG: InstanceName: {}, route: {}, topic only in manager: {}", instanceName, routeSet, topicOnlyInManager);
            }

            if (!controllerTopics.isEmpty() ) {
              validateWrongCount++;
              LOGGER.info("Validate WRONG: InstanceName: {}, route: {}, topic only in controller: {}", instanceName, routeSet, controllerTopics);
            }
          } catch (Exception e) {
            validateWrongCount++;
            LOGGER.warn("Validate WRONG: Get topics error when connecting to {} for route {}", instanceName, routeSet, e);
          }

          try {
            // try find worker mismatch between manager and controller
            String instanceResult = HttpClientUtils.getData(_httpClient, _requestConfig,
                instanceName, _controllerPort, "/instances");
            LOGGER.debug("Get workers from {}: {}", instanceName, instanceResult);
            JSONObject instanceResultJson = JSON.parseObject(instanceResult);
            JSONArray allInstances = instanceResultJson.getJSONArray("allInstances");
            Set<String> controllerWorkers = new HashSet<>();
            for (Object instance : allInstances) {
              controllerWorkers.add(String.valueOf(instance));
            }

            Set<String> managerWorkers = instanceMap.get(instanceName).getWorkerSet();
            Set<String> workerOnlyInManager = new HashSet<>();
            for (String worker : managerWorkers) {
              if (!controllerWorkers.contains(worker)) {
                workerOnlyInManager.add(worker);
              } else {
                controllerWorkers.remove(worker);
              }
            }

            if (!workerOnlyInManager.isEmpty()) {
              validateWrongCount++;
              LOGGER.info("Validate WRONG: InstanceName: {}, route: {}, worker only in manager: {}", instanceName, routeSet, workerOnlyInManager);
            }

            if (!controllerWorkers.isEmpty() ) {
              validateWrongCount++;
              LOGGER.info("Validate WRONG: InstanceName: {}, route: {}, worker only in controller: {}", instanceName, routeSet, controllerWorkers);
            }
          } catch (Exception e) {
            validateWrongCount++;
            LOGGER.warn("Validate WRONG: Get workers error when connecting to {} for route {}", instanceName, routeSet, e);
          }

        } else {
          validateWrongCount++;
          LOGGER.error("Validate WRONG: mismatch route found for InstanceName: {}, route: {}, mismatch: {}, #workers: {}, worker: {}",
              instanceName, routeSet, mismatchTopicPartition, instanceMap.get(instanceName).getWorkerSet().size(), instanceMap.get(instanceName).getWorkerSet());
        }
      }
    }
    LOGGER.info("\n\n");

    Map<String, Set<String>> topicToRouteMap = new HashMap<>();
    for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
      Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
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

    LOGGER.info("\n\nFor controller _pipelineToInstanceMap:");
    Map<String, Set<String>> workerMap = new HashMap<>();
    for (String pipeline: _pipelineToInstanceMap.keySet()) {
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
      updateMetrics(instanceToTopicPartitionsMap, instanceMap);
    }
  }

  private void updateMetrics(Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap,
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
      if (currTimeMs - lastUpdateTimeMs < 60000) {
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
          .getInstanceToTopicPartitionsMap(_helixManager, _srcKafkaValidationManager.getClusterToObserverMap());

      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      currAvailableControllerList.addAll(liveInstances);

      for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
        Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
        // TODO: one instance suppose to have only one route
        for (TopicPartition tp : topicPartitions) {
          String topicName = tp.getTopic();
          if (topicName.startsWith(SEPARATOR)) {
            currPipelineToInstanceMap.putIfAbsent(topicName, new PriorityQueue<>(1,
                InstanceTopicPartitionHolder.getTotalWorkloadComparator(_workloadInfoRetrieverMap.get(getSrc(topicName)), null, false)));
            InstanceTopicPartitionHolder itph = new InstanceTopicPartitionHolder(instanceName, tp);
            if (workerRouteToInstanceMap.get(tp) != null) {
              itph.addWorkers(workerRouteToInstanceMap.get(tp));
            }
            currPipelineToInstanceMap.get(topicName).add(itph);
            instanceMap.put(instanceName, itph);
            currAvailableControllerList.remove(instanceName);
          }
        }

        for (TopicPartition tp : topicPartitions) {
          String topicName = tp.getTopic();
          if (!topicName.startsWith(SEPARATOR)) {
            if (instanceMap.containsKey(instanceName)) {
              instanceMap.get(instanceName).addTopicPartition(tp);
              currTopicToPipelineInstanceMap.putIfAbsent(topicName, new ConcurrentHashMap<>());
              currTopicToPipelineInstanceMap.get(tp.getTopic()).put(getPipelineFromRoute(tp.getPipeline()),
                  instanceMap.get(instanceName));
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

  public JSONObject getTopicInfoFromController(String topicName) {
    JSONObject resultJson = new JSONObject();
    Map<String, InstanceTopicPartitionHolder> pipelineToInstanceMap = _topicToPipelineInstanceMap.get(topicName);
    for (String pipeline : pipelineToInstanceMap.keySet()) {
      InstanceTopicPartitionHolder itph = pipelineToInstanceMap.get(pipeline);
      try {
        String topicResponseBody = HttpClientUtils.getData(_httpClient, _requestConfig,
            itph.getInstanceName(), _controllerPort, "/topics/" + topicName);
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

  public synchronized void handleLiveInstanceChange(boolean onlyCheckOffline) throws Exception {
    _lock.lock();
    try {
      LOGGER.info("handleLiveInstanceChange() wake up!");

      // Check if any controller in route is down
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap = HelixUtils
          .getInstanceToTopicPartitionsMap(_helixManager, _srcKafkaValidationManager.getClusterToObserverMap());
      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      List<String> instanceToReplace = new ArrayList<>();
      boolean routeControllerDown = false;
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

            // TODO: check if _availableControllerList is empty
            String newInstanceName = _availableControllerList.get(0);
            _availableControllerList.remove(0);
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

      // Check if any worker in route is down
      HelixManager workeManager = _workerHelixManager.getHelixManager();
      Map<String, Set<TopicPartition>> workerInstanceToTopicPartitionsMap = HelixUtils
          .getInstanceToTopicPartitionsMap(workeManager, null);
      List<String> workerLiveInstances = HelixUtils.liveInstances(workeManager);
      Map<String, List<String>> workerPipelineToRouteIdToReplace = new HashMap<>();
      List<String> workerToReplace = new ArrayList<>();
      boolean routeWorkerDown = false;
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

      if (onlyCheckOffline) {
        return;
      }

      LOGGER.info("Start rebalancing current cluster");
      // Haven't run updateCurrentStatus() before
      if (!routeControllerDown && !routeWorkerDown) {
        updateCurrentStatus();
      }

      rebalanceCurrentCluster();

    } finally {
      _lock.unlock();
    }
  }

  private boolean isControllerOnline(String instance, String routeName, String routeId) {
    LOGGER.info("Check if {} is online for {}, {}", instance, routeName, routeId);
    try {
      String[] srcDst = routeName.split(SEPARATOR);
      String controllerWokerHelixClusterName = "/controller-worker-" + srcDst[1] + "-" + srcDst[2] + "-" + routeId;
      JSONObject json = JSON.parseObject(_zkClient.readData(controllerWokerHelixClusterName + "/CONTROLLER/LEADER").toString());
      String currLeader = String.valueOf(json.get("id"));
      LOGGER.info("current leader is {}, expect {}", currLeader, instance);
      return currLeader.equals(instance);
    } catch (Exception e) {
      LOGGER.info("Got error when checking current leader", e);
      return false;
    }
  }

  public void rebalanceCurrentCluster() throws Exception {
    int oldTotalNumWorker = 0;
    int newTotalNumWorker = 0;
    for (String pipeline : _pipelineToInstanceMap.keySet()) {
      LOGGER.info("Start rebalancing pipeline: {}", pipeline);
      PriorityQueue<InstanceTopicPartitionHolder> newItphQueue = new PriorityQueue<>(1,
          InstanceTopicPartitionHolder.getTotalWorkloadComparator(_workloadInfoRetrieverMap.get(getSrc(pipeline)), null, false));
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

        // TODO: what to do when workload retriever is not working
        // Expand route when topics are expanded
        /*if (itph.getTotalNumPartitions() > _initMaxNumPartitionsPerRoute) {
          LOGGER.info("Checking route {} with controller {} and topics {} since it exceeds "
                  + "initMaxNumPartitionsPerRoute {}", itph.getRouteString(), itph.getInstanceName(),
              itph.getServingTopicPartitionSet(), _initMaxNumPartitionsPerRoute);
          int expectedNumWorkers = getExpectedNumWorkers(itph.getTotalNumPartitions());
          LOGGER.info("current {}, expected {}", itph.getWorkerSet().size(), expectedNumWorkers);
          if (itph.getWorkerSet().size() < expectedNumWorkers) {
            LOGGER.info("Current {} workers in route {}, expect {} workers",
                itph.getWorkerSet().size(), itph.getRouteString(), expectedNumWorkers);
            // TODO: handle exception
            _workerHelixManager.addWorkersToMirrorMaker(itph, itph.getRoute().getTopic(),
                itph.getRoute().getPartition(), expectedNumWorkers - itph.getWorkerSet().size());
          }
        }*/

        // Expand route when configs are changed
        /*if (itph.getWorkerSet().size() < _initMaxNumWorkersPerRoute) {
          LOGGER.info("Checking route {} with controller {} and topics {} since its number of workers {} "
                  + "is smaller than initMaxNumWorkersPerRoute {}", itph.getRouteString(), itph.getInstanceName(),
              itph.getServingTopicPartitionSet(), itph.getWorkerSet().size(), _initMaxNumWorkersPerRoute);
          int expectedNumWorkers = _initMaxNumWorkersPerRoute;
          LOGGER.info("Current {}, expected {}", itph.getWorkerSet().size(), expectedNumWorkers);
          if (itph.getWorkerSet().size() < expectedNumWorkers) {
            LOGGER.info("Current {} workers in route {}, expect {} workers",
                itph.getWorkerSet().size(), itph.getRouteString(), expectedNumWorkers);
            // TODO: handle exception
            _workerHelixManager.addWorkersToMirrorMaker(itph, itph.getRoute().getTopic(),
                itph.getRoute().getPartition(), expectedNumWorkers - itph.getWorkerSet().size());
          }
        }*/

        newItphQueue.add(itph);
      }

      // After moving topics, scale workers based on workload
      for (InstanceTopicPartitionHolder itph : newItphQueue) {
        String cluster = itph.getSrc();
        WorkloadInfoRetriever retriever = _workloadInfoRetrieverMap.get(cluster);
        if (retriever == null) {
          String srcKafkaZkPath = (String) _conf.getProperty(CONFIG_KAFKA_CLUSTER_KEY_PREFIX + cluster);
          _workloadInfoRetrieverMap.put(cluster, new WorkloadInfoRetriever(this, srcKafkaZkPath));
          _workloadInfoRetrieverMap.get(cluster).start();
        }
        if (!retriever.isInitialized()) {
          LOGGER.info("Retriever for itph: {} not initialized", itph.getInstanceName());
          int actualNumWorkers = itph.getWorkerSet().size();
          if (_initMaxNumWorkersPerRoute > itph.getWorkerSet().size()) {
            LOGGER.info("Current {} workers in route {}, expect at least {} workers, add {} workers",
                itph.getWorkerSet().size(), itph.getRouteString(), _initMaxNumWorkersPerRoute, _initMaxNumWorkersPerRoute-itph.getWorkerSet().size());
            // TODO: handle exception
            _workerHelixManager.addWorkersToMirrorMaker(itph, itph.getRoute().getTopic(),
                itph.getRoute().getPartition(), _initMaxNumWorkersPerRoute - itph.getWorkerSet().size());
            actualNumWorkers = _initMaxNumWorkersPerRoute;
          }
          oldTotalNumWorker += itph.getWorkerSet().size();
          newTotalNumWorker += actualNumWorkers;
          continue;
        }
        TopicWorkload totalWorkload = itph.totalWorkload(_workloadInfoRetrieverMap.get(itph.getSrc()), null, false);
        double workloadPerWorker = itph.isSameDc() ? _initMaxWorkloadPerWorkerByteDc : _initMaxWorkloadPerWorkerByteXdc;
        LOGGER.info("itph: {}, route: {}, #topics: {}, #partitions: {}, totalWorkload: {}", itph.getInstanceName(), itph.getRouteString(),
            itph.getServingTopicPartitionSet().size(), itph.getTotalNumPartitions(), totalWorkload.getBytesPerSecond());
        int expectedNumWorkers = (int) Math.round(totalWorkload.getBytesPerSecond() / workloadPerWorker);
        LOGGER.info("Current {} workers in route {}, expect {} workers",
            itph.getWorkerSet().size(), itph.getRouteString(), expectedNumWorkers);
        int actualExpectedNumWorkers = getActualExpectedNumWorkers(expectedNumWorkers);
        LOGGER.info("Current {} workers in route {}, actual expect {} workers",
            itph.getWorkerSet().size(), itph.getRouteString(), actualExpectedNumWorkers);

        if (actualExpectedNumWorkers > itph.getWorkerSet().size()) {
          LOGGER.info("Current {} workers in route {}, actual expect {} workers, add {} workers",
              itph.getWorkerSet().size(), itph.getRouteString(), actualExpectedNumWorkers, actualExpectedNumWorkers-itph.getWorkerSet().size());
          // TODO: handle exception
          _workerHelixManager.addWorkersToMirrorMaker(itph, itph.getRoute().getTopic(),
              itph.getRoute().getPartition(), actualExpectedNumWorkers - itph.getWorkerSet().size());
        }

        if (actualExpectedNumWorkers < itph.getWorkerSet().size()) {
          LOGGER.info("Current {} workers in route {}, actual expect {} workers, remove {} workers",
              itph.getWorkerSet().size(), itph.getRouteString(), actualExpectedNumWorkers, itph.getWorkerSet().size()-actualExpectedNumWorkers);
          // TODO: handle exception
          _workerHelixManager.removeWorkersToMirrorMaker(itph, itph.getRoute().getTopic(),
              itph.getRoute().getPartition(), itph.getWorkerSet().size()-actualExpectedNumWorkers);
        }

        oldTotalNumWorker += itph.getWorkerSet().size();
        newTotalNumWorker += actualExpectedNumWorkers;
      }
      _pipelineToInstanceMap.put(pipeline, newItphQueue);
    }
    LOGGER.info("oldTotalNumWorker: {}, newTotalNumWorker: {}", oldTotalNumWorker, newTotalNumWorker);
  }

  private int getActualExpectedNumWorkers(int expectedNumWorkers) {
    if (expectedNumWorkers <= _initMaxNumWorkersPerRoute) {
      return _initMaxNumWorkersPerRoute;
    }
    if (expectedNumWorkers >= _maxNumWorkersPerRoute) {
      return _maxNumWorkersPerRoute;
    }
    return (int) (Math.ceil((double) (expectedNumWorkers - _initMaxNumWorkersPerRoute) / 5) * 5) + _initMaxNumWorkersPerRoute;
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
    String controllerWokerHelixClusterName = "controller-worker-" + srcDst[1] + "-" + srcDst[2] + "-" + routeId;
    HelixManager spectator = HelixManagerFactory.getZKHelixManager(controllerWokerHelixClusterName,
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
            instanceName, controllerWokerHelixClusterName));
      }
      Thread.sleep(1000);
    }

    _availableControllerList.remove(instanceName);
    _pipelineToInstanceMap.put(pipeline, new PriorityQueue<>(1,
        InstanceTopicPartitionHolder.getTotalWorkloadComparator(_workloadInfoRetrieverMap.get(srcDst[1]), null, false)));
    _pipelineToInstanceMap.get(pipeline).add(instance);
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
      String pipeline,
      boolean isSameDc) throws Exception {

    LOGGER.info("maybeCreateNewRoute, topicName: {}, numPartitions: {}, pipeline: {}", topicName, numPartitions,
        pipeline);

    double workload = isSameDc ? _initMaxWorkloadPerWorkerByteDc : _initMaxWorkloadPerWorkerByteXdc;
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

  public synchronized void addTopicToMirrorMaker(String topicName, int numPartitions,
      String src, String dst, String pipeline) throws Exception {
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
          numPartitions, pipeline, isSameDc);
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
  public synchronized void expandTopicInMirrorMaker(String topicName, String srcCluster, String pipeline,
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
      int respCode = HttpClientUtils.putData(_httpClient, _requestConfig,
          itph.getInstanceName(), _controllerPort, "/topics", entity);
      if (respCode != 200) {
        LOGGER.info("Got error from controller {} when expanding topic {} with respCode {}",
            itph.getInstanceName(), topicName, respCode);
        throw new Exception(String.format("Got error from controller %s when expanding topic %s with respCode %s",
            itph.getInstanceName(), topicName, respCode));
      }

      itph.removeTopicPartition(new TopicPartition(topicName, oldNumPartitions, pipeline));
      itph.addTopicPartition(new TopicPartition(topicName, newNumPartitions, pipeline));
      _srcKafkaValidationManager.getClusterToObserverMap().get(srcCluster).tryUpdateTopic(topicName);
    } finally {
      _lock.unlock();
    }
  }

  public synchronized void deletePipelineInMirrorMaker(String pipeline) {
    // TODO: delete topic first
    _lock.lock();
    try {
      LOGGER.info("Trying to delete pipeline: {}", pipeline);

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
      TopicPartition tp = _srcKafkaValidationManager.getClusterToObserverMap().get(src)
          .getTopicPartitionWithRefresh(topicName);
      instance.removeTopicPartition(tp);
      _topicToPipelineInstanceMap.get(topicName).remove(pipeline);
      if (instance.getServingTopicPartitionSet().isEmpty()) {
        _availableControllerList.add(instance.getInstanceName());
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

  public SourceKafkaClusterValidationManager getSrcKafkaValidationManager() {
    return _srcKafkaValidationManager;
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

}
