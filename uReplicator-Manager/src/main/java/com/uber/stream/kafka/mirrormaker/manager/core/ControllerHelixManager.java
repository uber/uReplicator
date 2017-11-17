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
import com.uber.stream.kafka.mirrormaker.common.configuration.IuReplicatorConf;
import com.uber.stream.kafka.mirrormaker.common.core.IHelixManager;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HttpClientUtils;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.validation.SourceKafkaClusterValidationManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
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
  private static final String SEPARATOR = "@";

  private final IuReplicatorConf _conf;
  private final SourceKafkaClusterValidationManager _srcKafkaValidationManager;
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final WorkerHelixManager _workerHelixManager;
  private LiveInstanceChangeListener _liveInstanceChangeListener;

  private final CloseableHttpClient _httpClient;
  private final int _controllerPort;
  private final RequestConfig _requestConfig;

  private final int _initMaxNumPartitionsPerRoute;
  private final int _maxNumPartitionsPerRoute;
  private final int _initMaxNumWorkersPerRoute;
  private final int _maxNumWorkersPerRoute;

  private ReentrantLock _lock = new ReentrantLock();
  private Map<String, Map<String, InstanceTopicPartitionHolder>> _topicToPipelineInstanceMap;
  private Map<String, PriorityQueue<InstanceTopicPartitionHolder>> _pipelineToInstanceMap;
  private List<String> _availableControllerList;

  public ControllerHelixManager(SourceKafkaClusterValidationManager srcKafkaValidationManager,
      ManagerConf managerConf) {
    _conf = managerConf;
    _srcKafkaValidationManager = srcKafkaValidationManager;
    _initMaxNumPartitionsPerRoute = managerConf.getInitMaxNumPartitionsPerRoute();
    _maxNumPartitionsPerRoute = managerConf.getMaxNumPartitionsPerRoute();
    _initMaxNumWorkersPerRoute = managerConf.getInitMaxNumWorkersPerRoute();
    _maxNumWorkersPerRoute = managerConf.getMaxNumWorkersPerRoute();
    _workerHelixManager = new WorkerHelixManager(managerConf);
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(managerConf.getManagerZkStr());
    _helixClusterName = MANAGER_CONTROLLER_HELIX_PREFIX + "-" + managerConf.getManagerDeployment();
    _instanceId = managerConf.getManagerInstanceId();
    _topicToPipelineInstanceMap = new ConcurrentHashMap<>();
    _pipelineToInstanceMap = new ConcurrentHashMap<>();
    _availableControllerList = new ArrayList<>();

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
        .setConnectionRequestTimeout(1000)
        .setConnectTimeout(1000)
        .setSocketTimeout(1000)
        .build();
  }

  public synchronized void start() {
    LOGGER.info("Trying to start ManagerControllerHelix!");

    _workerHelixManager.start();

    _helixManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixManager.getClusterManagmentTool();

    updateCurrentStatus();

    LOGGER.info("Trying to register ControllerLiveInstanceChangeListener");
    _liveInstanceChangeListener = new ControllerLiveInstanceChangeListener(this, _helixManager);
    try {
      _helixManager.addLiveInstanceChangeListener(_liveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add ControllerLiveInstanceChangeListener");
    }
  }

  public synchronized void stop() throws IOException {
    LOGGER.info("Trying to stop ManagerControllerHelix!");
    _workerHelixManager.stop();
    _helixManager.disconnect();
    _httpClient.close();
  }

  public synchronized void updateCurrentStatus() {
    _lock.lock();
    try {
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
      LOGGER.info("\n\nfor controller instanceToTopicPartitionsMap: {}\n\n", instanceToTopicPartitionsMap);
      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      currAvailableControllerList.addAll(liveInstances);

      for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
        Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
        // TODO: one instance suppose to have only one route
        for (TopicPartition tp : topicPartitions) {
          String topicName = tp.getTopic();
          if (topicName.startsWith(SEPARATOR)) {
            currPipelineToInstanceMap.putIfAbsent(topicName, new PriorityQueue<>(1,
                InstanceTopicPartitionHolder.getTotalWorkloadComparator(null, null)));
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
            instanceMap.get(instanceName).addTopicPartition(tp);
            currTopicToPipelineInstanceMap.putIfAbsent(topicName, new ConcurrentHashMap<>());
            currTopicToPipelineInstanceMap.get(tp.getTopic()).put(tp.getPipeline(), instanceMap.get(instanceName));
          }
        }
      }

      _pipelineToInstanceMap = currPipelineToInstanceMap;
      _topicToPipelineInstanceMap = currTopicToPipelineInstanceMap;
      _availableControllerList = currAvailableControllerList;

      LOGGER.info("For controller _pipelineToInstanceMap: {}", _pipelineToInstanceMap);
      LOGGER.info("For controller _topicToPipelineInstanceMap: {}", _topicToPipelineInstanceMap);
      LOGGER.info("For controller {} available, _availableControllerList: {}",
          _availableControllerList.size(), _availableControllerList);
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

  public void updateStatusFromController(InstanceTopicPartitionHolder itph) {
    try {
      LOGGER.info("get request");
      LOGGER.info("InstanceTopicPartitionHolder in updateStatusFromController: {}", itph);
      String topicResponseBody = HttpClientUtils.getData(_httpClient, _requestConfig,
          itph.getInstanceName(), _controllerPort, "/topics");
      if (topicResponseBody.startsWith("No topic")) {
        return;
      }

      List<String> tmpTopicList = extractTopicList(topicResponseBody);
      LOGGER.info("tmpTopicList: {}", tmpTopicList);
      String responseBody = HttpClientUtils.getData(_httpClient, _requestConfig,
          itph.getInstanceName(), _controllerPort, "/instances");
      JSONObject workerToTopicsInfoInJson = JSON.parseObject(responseBody).getJSONObject("instances");

      LOGGER.info("responseBody: {}", responseBody);

      Set<TopicPartition> topicList = new HashSet<>();
      Set<String> workerList = new HashSet<>();
      long workload = 0;

      for (String worker : workerToTopicsInfoInJson.keySet()) {
        JSONArray topicsInfo = workerToTopicsInfoInJson.getJSONArray(worker);
        for (int i = 0; i < topicsInfo.size(); i++) {
          String tpInfo = topicsInfo.get(i).toString();
          String[] tpToLoad = tpInfo.split(":");
          String topicName = tpToLoad[0].substring(0, tpToLoad[0].lastIndexOf("."));
          if (topicName.equals("TOTALWORKLOAD")) {
            workload += Long.valueOf(tpToLoad[1]);
          } else if (tmpTopicList.contains(topicName)) {
            topicList.add(new TopicPartition(topicName, -1));
          }
        }
      }
      itph.addTopicPartitions(topicList);
      itph.addWorkers(workerList);
    } catch (Exception e) {
      e.printStackTrace();
    }
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
      if (!routeControllerDown) {
        LOGGER.info("No controller in route is changed, do nothing!");
      } else {
        LOGGER.info("Controller need to replace: {}", instanceToReplace);
        for (String instance : instanceToReplace) {
          Set<TopicPartition> tpOrRouteSet = instanceToTopicPartitionsMap.get(instance);
          for (TopicPartition tpOrRoute : tpOrRouteSet) {
            if (tpOrRoute.getTopic().startsWith(SEPARATOR)) {
              String pipeline = tpOrRoute.getTopic();
              int routeId = tpOrRoute.getPartition();

              String newInstanceName = _availableControllerList.get(0);
              LOGGER.info("Controller {} in route {}@{} will be replaced by {}", instance, pipeline, routeId,
                  newInstanceName);
              InstanceTopicPartitionHolder newInstance = new InstanceTopicPartitionHolder(newInstanceName, tpOrRoute);

              List<TopicPartition> tpToReassign = new ArrayList<>();
              PriorityQueue<InstanceTopicPartitionHolder> itphList = _pipelineToInstanceMap.get(pipeline);
              for (InstanceTopicPartitionHolder itph : itphList) {
                if (itph.getInstanceName().equals(instance)) {
                  tpToReassign.addAll(itph.getServingTopicPartitionSet());
                }
              }
              _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
                  IdealStateBuilder
                      .resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                          pipeline, String.valueOf(routeId), newInstanceName));

              // Wait for controller to join
              if (!waitForExternalView(pipeline, String.valueOf(routeId))) {
                LOGGER.info("Failed to find controller {} in pipeline {} routeId {} online, drop it", newInstanceName,
                    pipeline, routeId);
                // TODO: make sense to set back?
                _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
                    IdealStateBuilder
                        .resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                            pipeline, String.valueOf(routeId), instance));
                throw new Exception(
                    String.format("Failed to find controller %s in ExternalView in pipeline %s routeId %s!",
                        newInstanceName, pipeline, routeId));
              }

              for (TopicPartition tp : tpToReassign) {
                _helixAdmin.setResourceIdealState(_helixClusterName, tp.getTopic(),
                    IdealStateBuilder
                        .resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, tp.getTopic()),
                            tp.getTopic(), pipeline + SEPARATOR + routeId, newInstanceName));
              }

              LOGGER
                  .info("Controller {} in route {}@{} is replaced by {}", instance, pipeline, routeId, newInstanceName);
              break;
            }
          }
        }
        updateCurrentStatus();
      }

      // Check if any worker in route is down
      HelixManager workeManager = _workerHelixManager.getHelixManager();
      Map<String, Set<TopicPartition>> workerInstanceToTopicPartitionsMap = HelixUtils
          .getInstanceToTopicPartitionsMap(workeManager, null);
      List<String> workerLiveInstances = HelixUtils.liveInstances(workeManager);
      LOGGER.info("on handle, workerLiveInstances: {}", workerLiveInstances);
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
        }
      }
      if (!routeWorkerDown) {
        LOGGER.info("No worker in route is changed, do nothing!");
      } else {
        LOGGER.info("Worker need to replace: {}, {}", workerToReplace, workerPipelineToRouteIdToReplace);
        _workerHelixManager.replaceWorkerInMirrorMaker(workerPipelineToRouteIdToReplace, workerToReplace);

        updateCurrentStatus();
      }
    } finally {
      _lock.unlock();
    }
  }

  public boolean waitForExternalView(String topicName, String partition) throws InterruptedException {
    long ts1 = System.currentTimeMillis();
    while (true) {
      ExternalView externalView = getExternalViewForTopic(topicName);
      if (externalView != null && externalView.getPartitionSet().contains(partition)) {
        Map<String, String> stateMap = externalView.getStateMap(partition);
        for (String server : stateMap.keySet()) {
          if (stateMap.get(server).equals("ONLINE")) {
            return true;
          }
        }
      }
      if (System.currentTimeMillis() - ts1 > 10000) {
        break;
      }
      LOGGER.info("Waiting for ExternalView for topic: {}, partition: {}", topicName, partition);
      Thread.sleep(1000);
    }
    return false;
  }

  public InstanceTopicPartitionHolder createNewRoute(String pipeline, int routeId) throws Exception {
    String instanceName = _availableControllerList.get(0);
    InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName,
        new TopicPartition(pipeline, routeId));
    boolean isNewPipeline = false;
    if (!isPipelineExisted(pipeline)) {
      isNewPipeline = true;
      setEmptyResourceConfig(pipeline);
      _helixAdmin.addResource(_helixClusterName, pipeline,
          IdealStateBuilder.buildCustomIdealStateFor(pipeline, String.valueOf(routeId), instance));
    } else {
      _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
          IdealStateBuilder.expandCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
              pipeline, String.valueOf(routeId), instance));
    }

    // Wait for controller to join
    if (!waitForExternalView(pipeline, String.valueOf(routeId))) {
      LOGGER.info("Failed to find controller {} in pipeline {} online, drop it", instanceName, pipeline);
      if (isNewPipeline) {
        _helixAdmin.dropResource(_helixClusterName, pipeline);
      } else {
        IdealState currIdealState = getIdealStateForTopic(pipeline);
        _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
            IdealStateBuilder.shrinkCustomIdealStateFor(currIdealState, pipeline, String.valueOf(routeId)));
      }
      throw new Exception(String.format("Failed to find controller %s in externalview in pipeline %s!",
          instanceName, pipeline));
    }

    _availableControllerList.remove(instanceName);
    _pipelineToInstanceMap.put(pipeline, new PriorityQueue<>(1,
        InstanceTopicPartitionHolder.getTotalWorkloadComparator(null, null)));
    _pipelineToInstanceMap.get(pipeline).add(instance);
    _workerHelixManager.addTopicToMirrorMaker(instance, pipeline, routeId);

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

  public synchronized void addTopicToMirrorMaker(String topicName, int numPartitions,
      String src, String dst, String pipeline) throws Exception {
    _lock.lock();
    try {
      LOGGER.info("Trying to add topic: {} to pipeline: {}", topicName, pipeline);

      if (_availableControllerList.isEmpty() || _workerHelixManager.getAvailableWorkerList().isEmpty()) {
        LOGGER.info("No available controller or worker!");
        throw new Exception("No available controller or worker!");
      }

      if (!isPipelineExisted(pipeline)) {
        createNewRoute(pipeline, 0);
      } else {
        LOGGER.info("Pipeline already existed!");
      }

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

      if (!waitForExternalView(topicName, route)) {
        throw new Exception(String.format("Failed to whitelist topic %s in route %s!", topicName, route));
      }

      instance.addTopicPartition(new TopicPartition(topicName, numPartitions, pipeline));
      _topicToPipelineInstanceMap.putIfAbsent(topicName, new ConcurrentHashMap<>());
      _topicToPipelineInstanceMap.get(topicName).put(pipeline, instance);
    } finally {
      _lock.unlock();
    }
  }

  public synchronized void deletePipelineMirrorMaker(String pipeline) {
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

}
