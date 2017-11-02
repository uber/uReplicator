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
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic for Helix Manager-Worker.
 *
 * @author hongxu
 */
public class WorkerHelixManager implements IHelixManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerHelixManager.class);

  private static final String MANAGER_WORKER_HELIX_PREFIX = "manager-worker";
  private static final String BLACKLIST_TAG = "blacklisted";

  private final IuReplicatorConf _conf;
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private final WorkloadInfoRetriever _workloadInfoRetriever;
  private final PriorityQueue<InstanceTopicPartitionHolder> _currentServingInstance;

  private LiveInstanceChangeListener _liveInstanceChangeListener;

  public WorkerHelixManager(ManagerConf managerConf) {
    _conf = managerConf;
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(managerConf.getManagerZkStr());
    _helixClusterName = MANAGER_WORKER_HELIX_PREFIX + "-" + managerConf.getManagerDeployment();
    _instanceId = managerConf.getManagerInstanceId();
    _workloadInfoRetriever = new WorkloadInfoRetriever(this);
    _currentServingInstance = new PriorityQueue<>(1,
        InstanceTopicPartitionHolder.getTotalWorkloadComparator(_workloadInfoRetriever, null));
  }

  public synchronized void start() {
    LOGGER.info("Trying to start ManagerWorkerHelix!");
    _helixManager = HelixSetupUtils.setup(_helixClusterName, _helixZkURL, _instanceId);
    _helixAdmin = _helixManager.getClusterManagmentTool();

    LOGGER.info("Trying to register WorkerLiveInstanceChangeListener");
    _liveInstanceChangeListener = new WorkerLiveInstanceChangeListener(this, _helixManager);
    try {
      _helixManager.addLiveInstanceChangeListener(_liveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add WorkerLiveInstanceChangeListener");
    }
  }

  public synchronized void stop() {
    LOGGER.info("Trying to stop ManagerWorkerHelix!");
    _helixManager.disconnect();
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

  public boolean isPipelineExisted(String pipeline) {
    return _helixAdmin.getResourcesInCluster(_helixClusterName).contains(pipeline);
  }

  public List<String> getTopicLists() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  public synchronized void addTopicToMirrorMaker(String pipeline) throws Exception {
    updateCurrentServingInstance();
    LOGGER.info("try to create route pipeline: {}", pipeline);
    if (!isPipelineExisted(pipeline)) {
      setEmptyResourceConfig(pipeline);
      synchronized (_currentServingInstance) {
        _helixAdmin.addResource(_helixClusterName, pipeline,
            IdealStateBuilder.buildCustomIdealStateFor(pipeline, "0", _currentServingInstance));
      }
    } else {
      LOGGER.info("worker pipeline existed!");
    }
  }

  public synchronized void deleteTopicInMirrorMaker(String topicName) {
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
