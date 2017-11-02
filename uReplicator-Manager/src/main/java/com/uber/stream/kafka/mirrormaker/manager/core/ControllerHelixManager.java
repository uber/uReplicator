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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
  private static final String BLACKLIST_TAG = "blacklisted";

  private final IuReplicatorConf _conf;
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final WorkerHelixManager _workerHelixManager;
  private final WorkloadInfoRetriever _workloadInfoRetriever;
  private final PriorityQueue<InstanceTopicPartitionHolder> _currentServingInstance;
  private LiveInstanceChangeListener _liveInstanceChangeListener;

  private final CloseableHttpClient _httpClient;
  private final int _controllerPort;
  private final RequestConfig _requestConfig;

  private final Map<String, Map<String, String>> _topicToPipelineInstanceMap;

  public ControllerHelixManager(ManagerConf managerConf) {
    _conf = managerConf;
    _workerHelixManager = new WorkerHelixManager(managerConf);
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(managerConf.getManagerZkStr());
    _helixClusterName = MANAGER_CONTROLLER_HELIX_PREFIX + "-" + managerConf.getManagerDeployment();
    _instanceId = managerConf.getManagerInstanceId();
    _workloadInfoRetriever = new WorkloadInfoRetriever(this);
    _currentServingInstance = new PriorityQueue<>(1,
        InstanceTopicPartitionHolder.getTotalWorkloadComparator(_workloadInfoRetriever, null));
    _topicToPipelineInstanceMap = new ConcurrentHashMap<>();

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
  }

  public synchronized void stop() throws IOException {
    LOGGER.info("Trying to stop ManagerControllerHelix!");
    _workerHelixManager.stop();
    _helixManager.disconnect();
    _httpClient.close();
  }

  public synchronized void updateCurrentServingInstance() {
    synchronized (_currentServingInstance) {
      Map<String, InstanceTopicPartitionHolder> instanceMap = new HashMap<>();
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap =
          HelixUtils.getInstanceToTopicPartitionsMap(_helixManager);
      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      Set<String> blacklistedInstances = new HashSet<>(getBlacklistedInstances());
      for (String instanceName : liveInstances) {
        if (!blacklistedInstances.contains(instanceName)) {
          InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName);
          instanceMap.put(instanceName, instance);
        }
      }
      for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
        if (instanceMap.containsKey(instanceName)) {
          instanceMap.get(instanceName).addTopicPartitions(instanceToTopicPartitionsMap.get(instanceName));
        }
      }
      _currentServingInstance.clear();
      int maxStandbyHosts = 0;
      int standbyHosts = 0;
      for (InstanceTopicPartitionHolder itph : instanceMap.values()) {
        if (standbyHosts >= maxStandbyHosts || itph.getNumServingTopicPartitions() > 0) {
          _currentServingInstance.add(itph);
        } else {
          // exclude it as a standby host
          standbyHosts++;
        }
      }
    }
  }

  public IdealState getIdealStateForTopic(String topicName) {
    return _helixAdmin.getResourceIdealState(_helixClusterName, topicName);
  }

  public ExternalView getExternalViewForTopic(String topicName) {
    return _helixAdmin.getResourceExternalView(_helixClusterName, topicName);
  }

  public boolean isPipelineExisted(String pipeline) {
    LOGGER.info("isPipelineExisted: {}", _helixAdmin.getResourcesInCluster(_helixClusterName));
    return _helixAdmin.getResourcesInCluster(_helixClusterName).contains(pipeline);
  }

  public boolean isTopicExisted(String topicName) {
    return _topicToPipelineInstanceMap.containsKey(topicName);
  }

  public boolean isTopicPipelineExisted(String topicName, String pipeline) {
    return _topicToPipelineInstanceMap.containsKey(topicName) &&
        _topicToPipelineInstanceMap.get(topicName).containsKey(pipeline);
  }

  public List<String> getTopicLists() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  public Map<String, String> getTopic(String topicName) {
    return _topicToPipelineInstanceMap.get(topicName);
  }

  public boolean isFinished(String topicName, String partition) {
    LOGGER.info("isFinished?");
    ExternalView externalView = getExternalViewForTopic(topicName);
    if (externalView == null || !externalView.getPartitionSet().contains(partition)) {
      return false;
    }

    Map<String, String> stateMap = externalView.getStateMap(partition);
    for (String server : stateMap.keySet()) {
      LOGGER.info(stateMap.get(server));
      if (stateMap.get(server).equals("ONLINE")) {
        return true;
      }
    }
    return false;
  }

  public synchronized void addTopicToMirrorMaker(String topicName, int numTopicPartitions, String src, String dst,
      String pipeline) throws Exception {
    updateCurrentServingInstance();
    LOGGER.info("try to create route topic: {} pipeline: {}", topicName, pipeline);
    InstanceTopicPartitionHolder instance = _currentServingInstance.peek();
    Map<String, String> p = new ConcurrentHashMap<>();
    if (!isPipelineExisted(pipeline)) {
      setEmptyResourceConfig(pipeline);
      synchronized (_currentServingInstance) {
        _helixAdmin.addResource(_helixClusterName, pipeline,
            IdealStateBuilder.buildCustomIdealStateFor(pipeline, "0", instance));
        p.put(pipeline, instance.getInstanceName());
      }
      _workerHelixManager.addTopicToMirrorMaker(pipeline);
    } else {
      LOGGER.info("existed!");
    }

    // TODO: wait for controller is joined.
    /*while (!isFinished(pipeline, "0")) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }*/

    try {
      LOGGER.info("rest request");
      int response = HttpClientUtils.postData(_httpClient, _requestConfig,
          instance.getInstanceName(), _controllerPort, topicName, src, dst, 0);
      if (response != 200) {
        throw new Exception("Error code: " + response);
      }
      _topicToPipelineInstanceMap.put(topicName, p);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public synchronized void deleteTopicInMirrorMaker(String topicName) {
    _workerHelixManager.deleteTopicInMirrorMaker(topicName);
    _helixAdmin.dropResource(_helixClusterName, topicName);
  }

  private synchronized void setEmptyResourceConfig(String topicName) {
    _helixAdmin.setConfig(new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(_helixClusterName)
        .forResource(topicName).build(), new HashMap<>());
  }

  public List<String> getBlacklistedInstances() {
    return _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, BLACKLIST_TAG);
  }

  public IuReplicatorConf getConf() {
    return _conf;
  }

}
