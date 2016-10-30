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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.HelixUtils;

/**
 * Main logic for Helix Controller. Provided all necessary APIs for topics management.
 * Have two modes auto/custom:
 * Auto mode is for helix taking care of all the idealStates changes
 * Custom mode is for creating a balanced idealStates when necessary,
 * like instances added/removed, new topic added/expanded, old topic deleted
 *
 * @author xiangfu
 */
public class HelixMirrorMakerManager {

  private static final String ENABLE = "enable";
  private static final String DISABLE = "disable";
  private static final String AUTO_BALANCING = "AutoBalancing";

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixMirrorMakerManager.class);

  private final ControllerConf _controllerConf;
  private final String _helixClusterName;
  private final String _helixZkURL;
  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final PriorityQueue<InstanceTopicPartitionHolder> _currentServingInstance =
      new PriorityQueue<InstanceTopicPartitionHolder>(1,
          InstanceTopicPartitionHolder.getComparator());

  public HelixMirrorMakerManager(ControllerConf controllerConf) {
    _controllerConf = controllerConf;
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(_controllerConf.getZkStr());
    _helixClusterName = _controllerConf.getHelixClusterName();
    _instanceId = controllerConf.getInstanceId();
  }

  public synchronized void start() {
    LOGGER.info("Trying to start HelixMirrorMakerManager!");
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    LOGGER.info("Trying to register AutoRebalanceLiveInstanceChangeListener");
    AutoRebalanceLiveInstanceChangeListener autoRebalanceLiveInstanceChangeListener =
        new AutoRebalanceLiveInstanceChangeListener(this, _helixZkManager,
            _controllerConf.getAutoRebalanceDelayInSeconds());
    updateCurrentServingInstance();
    try {
      _helixZkManager.addLiveInstanceChangeListener(autoRebalanceLiveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add LiveInstanceChangeListener");
    }
  }

  public synchronized void stop() {
    LOGGER.info("Trying to stop HelixMirrorMakerManager!");
    _helixZkManager.disconnect();
  }

  public synchronized void updateCurrentServingInstance() {
    synchronized (_currentServingInstance) {
      Map<String, InstanceTopicPartitionHolder> instanceMap =
          new HashMap<String, InstanceTopicPartitionHolder>();
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap =
          HelixUtils.getInstanceToTopicPartitionsMap(_helixZkManager);
      List<String> liveInstances = HelixUtils.liveInstances(_helixZkManager);
      for (String instanceName : liveInstances) {
        InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName);
        instanceMap.put(instanceName, instance);
      }
      for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
        if (instanceMap.containsKey(instanceName)) {
          instanceMap.get(instanceName)
              .addTopicPartitions(instanceToTopicPartitionsMap.get(instanceName));
        }
      }
      _currentServingInstance.clear();
      _currentServingInstance.addAll(instanceMap.values());
    }
  }

  public synchronized void addTopicToMirrorMaker(TopicPartition topicPartitionInfo) {
    this.addTopicToMirrorMaker(topicPartitionInfo.getTopic(), topicPartitionInfo.getPartition());
  }

  public synchronized void addTopicToMirrorMaker(String topicName, int numTopicPartitions) {
    setEmptyResourceConfig(topicName);
    updateCurrentServingInstance();
    synchronized (_currentServingInstance) {
      _helixAdmin.addResource(_helixClusterName, topicName,
          IdealStateBuilder.buildCustomIdealStateFor(topicName, numTopicPartitions,
              _currentServingInstance));
    }
  }

  private synchronized void setEmptyResourceConfig(String topicName) {
    _helixAdmin.setConfig(
        new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(_helixClusterName)
            .forResource(topicName).build(),
        new HashMap<String, String>());
  }

  public synchronized void expandTopicInMirrorMaker(TopicPartition topicPartitionInfo) {
    this.expandTopicInMirrorMaker(topicPartitionInfo.getTopic(), topicPartitionInfo.getPartition());
  }

  public synchronized void expandTopicInMirrorMaker(String topicName, int newNumTopicPartitions) {
    updateCurrentServingInstance();
    synchronized (_currentServingInstance) {
      _helixAdmin.setResourceIdealState(_helixClusterName, topicName,
          IdealStateBuilder.expandCustomRebalanceModeIdealStateFor(
              _helixAdmin.getResourceIdealState(_helixClusterName, topicName), topicName,
              newNumTopicPartitions,
              _currentServingInstance));
    }
  }

  public synchronized void deleteTopicInMirrorMaker(String topicName) {
    _helixAdmin.dropResource(_helixClusterName, topicName);
  }

  public IdealState getIdealStateForTopic(String topicName) {
    return _helixAdmin.getResourceIdealState(_helixClusterName, topicName);
  }

  public ExternalView getExternalViewForTopic(String topicName) {
    return _helixAdmin.getResourceExternalView(_helixClusterName, topicName);
  }

  public boolean isTopicExisted(String topicName) {
    return _helixAdmin.getResourcesInCluster(_helixClusterName).contains(topicName);
  }

  public List<String> getTopicLists() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  public void disableAutoBalancing() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(_helixClusterName)
            .build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(AUTO_BALANCING, DISABLE);
    _helixAdmin.setConfig(scope, properties);
  }

  public void enableAutoBalancing() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(_helixClusterName)
            .build();
    Map<String, String> properties = new HashMap<String, String>();
    properties.put(AUTO_BALANCING, ENABLE);
    _helixAdmin.setConfig(scope, properties);
  }

  public boolean isAutoBalancingEnabled() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(_helixClusterName)
            .build();
    Map<String, String> config = _helixAdmin.getConfig(scope, Arrays.asList(AUTO_BALANCING));
    if (config.containsKey(AUTO_BALANCING) && config.get(AUTO_BALANCING).equals(DISABLE)) {
      return false;
    }
    return true;
  }

  public boolean isLeader() {
    return _helixZkManager.isLeader();
  }

  public List<LiveInstance> getCurrentLiveInstances() {
    HelixDataAccessor helixDataAccessor = _helixZkManager.getHelixDataAccessor();
    PropertyKey liveInstancePropertyKey = new Builder(_helixClusterName).liveInstances();
    List<LiveInstance> liveInstances = helixDataAccessor.getChildValues(liveInstancePropertyKey);
    return liveInstances;
  }

  public String getHelixZkURL() {
      return _helixZkURL;
  }

  public String getHelixClusterName() {
      return _helixClusterName;
  }

}
