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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.google.common.annotations.VisibleForTesting;
import com.uber.stream.kafka.mirrormaker.common.Constants;
import com.uber.stream.kafka.mirrormaker.common.configuration.IuReplicatorConf;
import com.uber.stream.kafka.mirrormaker.common.core.IHelixManager;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.core.WorkloadInfoRetriever;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.uber.stream.kafka.mirrormaker.common.modules.TopicPartitionLag;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic for Helix Controller. Provided all necessary APIs for topics management. Have two modes auto/custom: Auto
 * mode is for helix taking care of all the idealStates changes Custom mode is for creating a balanced idealStates when
 * necessary, like instances added/removed, new topic added/expanded, old topic deleted
 *
 * @author xiangfu
 */
public class HelixMirrorMakerManager implements IHelixManager {

  private static final String ENABLE = "enable";
  private static final String DISABLE = "disable";
  private static final String AUTO_BALANCING = "AutoBalancing";
  private static final String BLACKLIST_TAG = "blacklisted";

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixMirrorMakerManager.class);

  private final ControllerConf _controllerConf;
  private final String _helixClusterName;
  private final String _helixZkURL;

  private HelixManager _helixZkManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final PriorityQueue<InstanceTopicPartitionHolder> _currentServingInstance;

  private AutoRebalanceLiveInstanceChangeListener _autoRebalanceLiveInstanceChangeListener = null;

  // if the difference between latest offset and commit offset is less than this
  // threshold, it is not considered as lag
  private final long _minLagOffset;

  // if the difference between latest offset and commit offset is less than this
  // threshold in terms of ingestion time, it is not considered as lag
  private final long _minLagTimeSec;

  // the maximum valid time for offset
  private final long _offsetMaxValidTimeMillis;

  // the maximum ratio of instances can be used as dedicated for lagging partitions
  private final double _maxDedicatedInstancesRatio;

  private double _maxWorkloadPerWorkerBytes;

  @VisibleForTesting
  WorkloadInfoRetriever _workloadInfoRetriever;

  @VisibleForTesting
  OffsetMonitor _offsetMonitor;

  public double getMaxWorkloadPerWorkerBytes() {
    return _maxWorkloadPerWorkerBytes;
  }

  public double getMaxDedicatedInstancesRatio() {
    return _maxDedicatedInstancesRatio;
  }

  public HelixMirrorMakerManager(ControllerConf controllerConf) {
    _controllerConf = controllerConf;
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(_controllerConf.getZkStr());
    _helixClusterName = _controllerConf.getHelixClusterName();
    _instanceId = controllerConf.getInstanceId();
    _workloadInfoRetriever = new WorkloadInfoRetriever(this, true);
    _currentServingInstance = new PriorityQueue<>(1,
        InstanceTopicPartitionHolder.perPartitionWorkloadComparator(_workloadInfoRetriever, null));
    _offsetMonitor = new OffsetMonitor(this, controllerConf);

    _minLagTimeSec = controllerConf.getAutoRebalanceMinLagTimeInSeconds();
    _minLagOffset = controllerConf.getAutoRebalanceMinLagOffset();
    _offsetMaxValidTimeMillis = TimeUnit.SECONDS.toMillis(controllerConf.getAutoRebalanceMaxOffsetInfoValidInSeconds());
    _maxDedicatedInstancesRatio = controllerConf.getMaxDedicatedLaggingInstancesRatio();
  }

  public synchronized void start() {
    LOGGER.info("Trying to start HelixMirrorMakerManager!");
    _helixZkManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixZkManager.getClusterManagmentTool();
    LOGGER.info("Trying to register AutoRebalanceLiveInstanceChangeListener");
    _autoRebalanceLiveInstanceChangeListener = new AutoRebalanceLiveInstanceChangeListener(this, _helixZkManager,
        _controllerConf);
    _maxWorkloadPerWorkerBytes =
        isSameRegion(_controllerConf.getSourceCluster(), _controllerConf.getDestinationCluster()) ?
            _controllerConf.getMaxWorkloadPerWorkerByteWithinRegion()
            : _controllerConf.getMaxWorkloadPerWorkerByteCrossRegion();
    LOGGER.info("environment:maxWorkloadPerWorkerBytes {}", _maxWorkloadPerWorkerBytes);
    updateCurrentServingInstance();
    _workloadInfoRetriever.start();
    _offsetMonitor.start();

    try {
      _helixZkManager.addLiveInstanceChangeListener(_autoRebalanceLiveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add LiveInstanceChangeListener");
    }
  }

  @VisibleForTesting
  void start(HelixManager helixZKManager, HelixAdmin helixAdmin) {
    _helixZkManager = helixZKManager;
    _helixAdmin = helixAdmin;
  }

  public synchronized void stop() {
    LOGGER.info("Trying to stop HelixMirrorMakerManager!");
    if (_autoRebalanceLiveInstanceChangeListener != null) {
      _autoRebalanceLiveInstanceChangeListener.stop();
    }
    _workloadInfoRetriever.stop();
    try {
      _offsetMonitor.stop();
    } catch (InterruptedException e) {
      LOGGER.info("Stopping kafkaMonitor got interrupted.");
    }
    _helixZkManager.disconnect();
  }

  public synchronized void updateCurrentServingInstance() {
    synchronized (_currentServingInstance) {
      Map<String, InstanceTopicPartitionHolder> instanceMap = new HashMap<>();
      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap =
          HelixUtils.getInstanceToTopicPartitionsMap(_helixZkManager);
      List<String> liveInstances = HelixUtils.liveInstances(_helixZkManager);
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
      int maxStandbyHosts = (_controllerConf.getMaxWorkingInstances() <= 0) ? 0
          : instanceMap.size() - _controllerConf.getMaxWorkingInstances();
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
        new HashMap<>());
  }

  public synchronized void expandTopicInMirrorMaker(TopicPartition topicPartitionInfo) {
    this.expandTopicInMirrorMaker(topicPartitionInfo.getTopic(), topicPartitionInfo.getPartition());
  }

  public synchronized void expandTopicInMirrorMaker(String topicName, int newNumTopicPartitions) {
    if (!isTopicExisted(topicName)) {
      LOGGER.warn("Skip expandTopicInMirrorMaker for topic {}, newNumTopicPartitions {} because of "
          + "topic doesn't exists in current pipeline", topicName, newNumTopicPartitions);
      return;
    }
    IdealState oldIdealState = _helixAdmin.getResourceIdealState(_helixClusterName, topicName);
    int numOldPartitions = oldIdealState.getNumPartitions();
    if (numOldPartitions >= newNumTopicPartitions) {
      LOGGER.info(
          "Skip expand topic {} because of numOldPartitions {} is greater or the same as newNumTopicPartitions {}",
          topicName, numOldPartitions, newNumTopicPartitions);
      return;
    }

    updateCurrentServingInstance();
    synchronized (_currentServingInstance) {
      _helixAdmin.setResourceIdealState(_helixClusterName, topicName,
          IdealStateBuilder.expandCustomRebalanceModeIdealStateFor(
              oldIdealState, topicName,
              newNumTopicPartitions, _controllerConf, _currentServingInstance));
    }
  }

  public Set<TopicPartition> getTopicPartitionBlacklist() {
    Set<TopicPartition> topicPartitionBlacklist = new HashSet<>();
    List<String> topicList = _helixAdmin.getResourcesInCluster(_helixClusterName);
    for (String topic : topicList) {
      IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName, topic);
      int numPartitions = is.getNumPartitions();
      for (int i = 0; i < numPartitions; i++) {
        Map<String, String> stateMap = is.getInstanceStateMap(String.valueOf(i));
        if (stateMap != null && stateMap.values().iterator().hasNext() &&
            stateMap.values().iterator().next().equalsIgnoreCase(Constants.HELIX_OFFLINE_STATE)) {
          topicPartitionBlacklist.add(new TopicPartition(topic, i));
        }
      }
    }
    return topicPartitionBlacklist;
  }

  public synchronized void updateTopicPartitionStateInMirrorMaker(String topicName, int partition, String state) {
    updateCurrentServingInstance();
    if (!Constants.HELIX_OFFLINE_STATE.equalsIgnoreCase(state) && !Constants.HELIX_ONLINE_STATE
        .equalsIgnoreCase(state)) {
      throw new IllegalArgumentException(String.format("Failed to update topic %s, partition %d to invalid state %s.",
          topicName, partition, state));
    }

    IdealState idealState = _helixAdmin.getResourceIdealState(_helixClusterName, topicName);
    String partitionName = String.valueOf(partition);
    if (idealState == null ||
        partition >= idealState.getNumPartitions()) {
      throw new IllegalArgumentException(String.format("Topic %s, partition %d not exists in current route.",
          topicName, partition));
    }

    String instanceName;
    if (idealState.getInstanceStateMap(partitionName).keySet().isEmpty()) {
      if (Constants.HELIX_OFFLINE_STATE.equalsIgnoreCase(state)) {
        throw new IllegalArgumentException(String.format("Topic %s, partition %d not exists in current route.",
            topicName, partition));
      } else if (_currentServingInstance.isEmpty()) {
        throw new InternalError("No available worker");
      }
      instanceName = _currentServingInstance.poll().getInstanceName();
    } else {
      instanceName = idealState.getInstanceStateMap(partitionName).keySet().iterator().next();
      String oldState = idealState.getInstanceStateMap(partitionName).get(instanceName);
      if (oldState.equalsIgnoreCase(state)) {
        throw new IllegalArgumentException(String.format("Topic %s, partition %d already set %s",
            idealState.getResourceName(), partition, state));
      }
    }

    idealState.setPartitionState(partitionName, instanceName, state);
    _helixAdmin.setResourceIdealState(_helixClusterName, topicName, idealState);
  }

  public synchronized void deleteTopicInMirrorMaker(String topicName) {
    _helixAdmin.dropResource(_helixClusterName, topicName);
  }

  @Override
  public IdealState getIdealStateForTopic(String topicName) {
    return _helixAdmin.getResourceIdealState(_helixClusterName, topicName);
  }

  @Override
  public IuReplicatorConf getConf() {
    return _controllerConf;
  }

  public ExternalView getExternalViewForTopic(String topicName) {
    return _helixAdmin.getResourceExternalView(_helixClusterName, topicName);
  }

  public boolean isTopicExisted(String topicName) {
    return _helixAdmin.getResourcesInCluster(_helixClusterName).contains(topicName);
  }

  @Override
  public List<String> getTopicLists() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  public void disableAutoBalancing() {
    HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER)
        .forCluster(_helixClusterName).build();
    Map<String, String> properties = new HashMap<>();
    properties.put(AUTO_BALANCING, DISABLE);
    _helixAdmin.setConfig(scope, properties);
  }

  public void enableAutoBalancing() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(_helixClusterName)
            .build();
    Map<String, String> properties = new HashMap<>();
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
    return HelixUtils.getCurrentLiveInstances(_helixZkManager);
  }

  public List<String> getHelixDisableInstances() {
    return HelixUtils.getHelixDisabledInstanceNames(_helixZkManager);
  }

  public List<String> getCurrentLiveInstanceNames() {
    return HelixUtils.getCurrentLiveInstanceNames(_helixZkManager);
  }

  public void blacklistInstance(String instanceName) {
    _helixAdmin.addInstanceTag(_helixClusterName, instanceName, BLACKLIST_TAG);
  }

  public void whitelistInstance(String instanceName) {
    _helixAdmin.removeInstanceTag(_helixClusterName, instanceName, BLACKLIST_TAG);
  }

  public List<String> getBlacklistedInstances() {
    return _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, BLACKLIST_TAG);
  }

  public String getHelixZkURL() {
    return _helixZkURL;
  }

  public String getHelixClusterName() {
    return _helixClusterName;
  }

  public ControllerConf getControllerConf() {
    return _controllerConf;
  }

  public WorkloadInfoRetriever getWorkloadInfoRetriever() {
    return _workloadInfoRetriever;
  }

  public AutoRebalanceLiveInstanceChangeListener getRebalancer() {
    return _autoRebalanceLiveInstanceChangeListener;
  }

  public PriorityQueue<InstanceTopicPartitionHolder> getCurrentServingInstance() {
    updateCurrentServingInstance();
    return _currentServingInstance;
  }

  public OffsetMonitor getOffsetMonitor() {
    return _offsetMonitor;
  }

  public boolean isHealthy() {
    return getOffsetMonitor().isHealthy();
  }

  /**
   * Calculates the lagging time if the given partition has lag.
   *
   * @param tp topic partition
   * @return the lagging time in seconds if the given partition has lag; otherwise return null.
   */
  public TopicPartitionLag calculateLagTime(TopicPartition tp) {
    TopicPartitionLag tpl = getOffsetMonitor().getTopicPartitionOffset(tp);
    if (tpl == null || tpl.getLatestOffset() <= 0 || tpl.getCommitOffset() <= 0
        || System.currentTimeMillis() - tpl.getTimeStamp() > _offsetMaxValidTimeMillis) {
      return null;
    }
    long lag = tpl.getLatestOffset() - tpl.getCommitOffset();
    if (lag <= _minLagOffset) {
      return null;
    }

    double msgRate = getWorkloadInfoRetriever().topicWorkload(tp.getTopic())
        .getMsgsPerSecondPerPartition();
    if (msgRate < 1) {
      msgRate = 1;
    }
    double lagTime = lag / msgRate;
    if (lagTime > _minLagTimeSec) {
      tpl.setLagTime(Math.round(lagTime));
      return tpl;
    }
    return null;
  }

  public boolean isSameRegion(String src, String dst) {
    if (StringUtils.isBlank(src) || StringUtils.isBlank(dst)) {
      LOGGER.error("Either srcCluster: {} or dstCluster: {} is empty, return false", src, dst);
      return false;
    }
    return src.substring(0, 3).equals(dst.substring(0, 3));
  }
}
