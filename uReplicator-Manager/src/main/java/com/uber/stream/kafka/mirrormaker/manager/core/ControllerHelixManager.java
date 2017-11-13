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
import com.uber.stream.kafka.mirrormaker.common.core.WorkloadInfoRetriever;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HttpClientUtils;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
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
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final WorkerHelixManager _workerHelixManager;
  private final WorkloadInfoRetriever _workloadInfoRetriever;
  private LiveInstanceChangeListener _liveInstanceChangeListener;

  private final CloseableHttpClient _httpClient;
  private final int _controllerPort;
  private final RequestConfig _requestConfig;

  private ReentrantLock lock = new ReentrantLock();
  private Map<String, Map<String, InstanceTopicPartitionHolder>> _topicToPipelineInstanceMap;
  private Map<String, PriorityQueue<InstanceTopicPartitionHolder>> _pipelineToInstanceMap;
  private List<String> _availableControllerSet;

  public ControllerHelixManager(ManagerConf managerConf) {
    _conf = managerConf;
    _workerHelixManager = new WorkerHelixManager(managerConf);
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(managerConf.getManagerZkStr());
    _helixClusterName = MANAGER_CONTROLLER_HELIX_PREFIX + "-" + managerConf.getManagerDeployment();
    _instanceId = managerConf.getManagerInstanceId();
    _workloadInfoRetriever = new WorkloadInfoRetriever(this);
    _topicToPipelineInstanceMap = new ConcurrentHashMap<>();
    _pipelineToInstanceMap = new ConcurrentHashMap<>();
    _availableControllerSet = new ArrayList<>();

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

    LOGGER.info("Trying to register ControllerLiveInstanceChangeListener");
    _liveInstanceChangeListener = new ControllerLiveInstanceChangeListener(this, _helixManager);
    try {
      _helixManager.addLiveInstanceChangeListener(_liveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add ControllerLiveInstanceChangeListener");
    }

    updateCurrentStatus();
  }

  public synchronized void stop() throws IOException {
    LOGGER.info("Trying to stop ManagerControllerHelix!");
    _workerHelixManager.stop();
    _helixManager.disconnect();
    _httpClient.close();
  }

  public synchronized void updateCurrentStatus() {
    LOGGER.info("Trying to updateCurrentStatus");

    // Map<InstanceName, InstanceTopicPartitionHolder>
    Map<String, InstanceTopicPartitionHolder> instanceMap = new HashMap<>();
    // Map<TopicName, Map<Pipeline, Instance>>
    Map<String, Map<String, InstanceTopicPartitionHolder>> currTopicToPipelineInstanceMap = new HashMap<>();
    // Map<Pipeline, PriorityQueue<Instance>>
    Map<String, PriorityQueue<InstanceTopicPartitionHolder>> currPipelineToInstanceMap = new HashMap<>();
    // Set<InstanceName>
    List<String> currAvailableControllerSet = new ArrayList<>();

    // Map<Instance, Set<Pipeline>> from IdealState
    Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap = HelixUtils
        .getInstanceToTopicPartitionsMap(_helixManager);
    List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
    currAvailableControllerSet.addAll(liveInstances);

    for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
      Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
      // TODO: one instance suppose to have only one route
      for (TopicPartition tp : topicPartitions) {
        String topicName = tp.getTopic();
        if (topicName.startsWith("@")) {
          if (!currPipelineToInstanceMap.containsKey(topicName)) {
            currPipelineToInstanceMap.put(tp.getTopic(), new PriorityQueue<>(1,
                InstanceTopicPartitionHolder.getTotalWorkloadComparator(_workloadInfoRetriever, null)));
          }
          InstanceTopicPartitionHolder itph = new InstanceTopicPartitionHolder(instanceName, tp);
          currPipelineToInstanceMap.get(topicName).add(itph);
          instanceMap.put(instanceName, itph);
          currAvailableControllerSet.remove(instanceName);
        }
      }

      for (TopicPartition tp : topicPartitions) {
        String topicName = tp.getTopic();
        if (!topicName.startsWith("@")) {
          instanceMap.get(instanceName).addTopicPartition(tp);
          if (!currTopicToPipelineInstanceMap.containsKey(topicName)) {
            currTopicToPipelineInstanceMap.put(topicName, new ConcurrentHashMap<>());
          }
          currTopicToPipelineInstanceMap.get(tp.getTopic()).put(tp.getPipeline(), instanceMap.get(instanceName));
        }
      }
    }

    lock.lock();
    try {
      _pipelineToInstanceMap = currPipelineToInstanceMap;
      _topicToPipelineInstanceMap = currTopicToPipelineInstanceMap;
      _availableControllerSet = currAvailableControllerSet;
    } finally {
      lock.unlock();
    }
    LOGGER.info("1 _pipelineToInstanceMap: {}", _pipelineToInstanceMap);
    LOGGER.info("2 _topicToPipelineInstanceMap: {}", _topicToPipelineInstanceMap);
    LOGGER.info("3 _availableControllerSet: {}", _availableControllerSet);
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
      itph.setWoakload(workload);
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
      if (resource.startsWith("@")) {
        pipelineList.add(resource);
      }
    }
    return pipelineList;
  }

  public List<String> getTopicLists() {
    List<String> toplicList = new ArrayList<>();
    for (String resource : _helixAdmin.getResourcesInCluster(_helixClusterName)) {
      if (!resource.startsWith("@")) {
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

  public boolean waitForExternalView(String topicName, String partition) throws InterruptedException {
    long ts1 = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - ts1 > 10000) {
        break;
      }
      ExternalView externalView = getExternalViewForTopic(topicName);
      if (externalView == null || !externalView.getPartitionSet().contains(partition)) {
        continue;
      }

      Map<String, String> stateMap = externalView.getStateMap(partition);
      for (String server : stateMap.keySet()) {
        if (stateMap.get(server).equals("ONLINE")) {
          return true;
        }
      }
      LOGGER.info("Waiting for externalview for topic: {}, partition: {}", topicName, partition);
      Thread.sleep(1000);
    }
    return false;
  }

  public synchronized void addTopicToMirrorMaker(String topicName, int numTopicPartitions,
      String src, String dst, String pipeline) throws Exception {
    LOGGER.info("Trying to add topic: {} to pipeline: {}", topicName, pipeline);

    if (!isPipelineExisted(pipeline)) {
      setEmptyResourceConfig(pipeline);
      lock.lock();
      try {
        if (!_availableControllerSet.isEmpty()) {
          String instanceName = _availableControllerSet.get(0);
          _availableControllerSet.remove(instanceName);
          InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName,
              new TopicPartition(pipeline, 0));
          _helixAdmin.addResource(_helixClusterName, pipeline,
              IdealStateBuilder.buildCustomIdealStateFor(pipeline, "0", instance));
          _pipelineToInstanceMap.put(pipeline, new PriorityQueue<>(1,
              InstanceTopicPartitionHolder.getTotalWorkloadComparator(_workloadInfoRetriever, null)));
          _pipelineToInstanceMap.get(pipeline).add(instance);
        } else {
          throw new Exception("No available controller!");
        }
      } finally {
        lock.unlock();
      }
      _workerHelixManager.addTopicToMirrorMaker(pipeline);
    } else {
      LOGGER.info("Pipeline already existed!");
    }

    // Wait for controller to join
    if (!waitForExternalView(pipeline, "0")) {
      throw new Exception(String.format("Failed to found controller %s in externalview in route %s!",
          _pipelineToInstanceMap.get(pipeline).peek().getInstanceName(), pipeline + "@0"));
    }

    // TODO: get proper InstanceTopicPartitionHolder
    InstanceTopicPartitionHolder instance = _pipelineToInstanceMap.get(pipeline).peek();
    String partition = pipeline + "@0";
    if (!isTopicExisted(topicName)) {
      setEmptyResourceConfig(topicName);
      _helixAdmin.addResource(_helixClusterName, topicName,
          IdealStateBuilder.buildCustomIdealStateFor(topicName, partition, instance));
    } else {
      _helixAdmin.setResourceIdealState(_helixClusterName, topicName,
          IdealStateBuilder.expandCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, topicName),
              topicName, partition, instance));
    }
    instance.addTopicPartition(new TopicPartition(topicName, -1, pipeline));

    if (!waitForExternalView(topicName, partition)) {
      throw new Exception(String.format("Failed to whitelist topic %s in route %s!", topicName, partition));
    }

    lock.lock();
    try {
      if (!_topicToPipelineInstanceMap.containsKey(topicName)) {
        _topicToPipelineInstanceMap.put(topicName, new ConcurrentHashMap<>());
      }
      _topicToPipelineInstanceMap.get(topicName).put(pipeline, instance);
    } finally {
      lock.unlock();
    }
  }

  public synchronized void deletePipelineMirrorMaker(String pipeline) {
    LOGGER.info("Trying to delete pipeline: {}", pipeline);

    // TODO: delete topic first
    _workerHelixManager.deleteTopicInMirrorMaker(pipeline);
    _helixAdmin.dropResource(_helixClusterName, pipeline);
    lock.lock();
    try {
      _pipelineToInstanceMap.remove(pipeline);
      for (String topic : _topicToPipelineInstanceMap.keySet()) {
        if (_topicToPipelineInstanceMap.get(topic).containsKey(pipeline)) {
          _topicToPipelineInstanceMap.get(topic).remove(pipeline);
        }
        if (_topicToPipelineInstanceMap.get(topic).isEmpty()) {
          _topicToPipelineInstanceMap.remove(topic);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public synchronized void deleteTopicInMirrorMaker(String topicName, String src, String dst, String pipeline)
      throws Exception {
    LOGGER.info("Trying to delete topic: {} in pipeline: {}", topicName, pipeline);
    lock.lock();
    try {
      InstanceTopicPartitionHolder instance = _topicToPipelineInstanceMap.get(topicName).get(pipeline);
      IdealState currIdealState = getIdealStateForTopic(topicName);
      if (currIdealState.getPartitionSet().contains(instance.getRouteString())
          && currIdealState.getNumPartitions() == 1) {
        _helixAdmin.dropResource(_helixClusterName, topicName);
      } else {
        _helixAdmin.setResourceIdealState(_helixClusterName, topicName,
            IdealStateBuilder.shrinkCustomIdealStateFor(currIdealState, topicName,
                    instance.getRouteString(), instance));
      }
      instance.removeTopicPartition(new TopicPartition(topicName, -1));
      _topicToPipelineInstanceMap.get(topicName).remove(pipeline);
      if (instance.getServingTopicPartitionSet().isEmpty()) {
        _availableControllerSet.add(instance.getInstanceName());
      }
    } finally {
      lock.unlock();
    }
  }

  private synchronized void setEmptyResourceConfig(String topicName) {
    _helixAdmin.setConfig(new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE)
        .forCluster(_helixClusterName)
        .forResource(topicName).build(), new HashMap<>());
  }

  public IuReplicatorConf getConf() {
    return _conf;
  }

}
