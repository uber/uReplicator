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
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
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
  private static final String SEPARATOR = "@";

  private final ManagerConf _conf;
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private LiveInstanceChangeListener _liveInstanceChangeListener;

  private ReentrantLock _lock = new ReentrantLock();
  private Map<TopicPartition, List<String>> _routeToInstanceMap;
  private List<String> _availableWorkerList;

  public WorkerHelixManager(ManagerConf managerConf) {
    _conf = managerConf;
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(managerConf.getManagerZkStr());
    _helixClusterName = MANAGER_WORKER_HELIX_PREFIX + "-" + managerConf.getManagerDeployment();
    _instanceId = managerConf.getManagerInstanceId();
    _routeToInstanceMap = new ConcurrentHashMap<>();
    _availableWorkerList = new ArrayList<>();
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

  public synchronized void updateCurrentStatus() {
    LOGGER.info("Trying to run worker updateCurrentStatus");
    _lock.lock();
    try {
      Map<TopicPartition, List<String>> currRouteToInstanceMap = new HashMap<>();
      List<String> currAvailableWorkerList = new ArrayList<>();

      Map<String, Set<TopicPartition>> instanceToTopicPartitionsMap = HelixUtils
          .getInstanceToTopicPartitionsMap(_helixManager);
      LOGGER.info("\n\nfor worker instanceToTopicPartitionsMap: {}\n\n", instanceToTopicPartitionsMap);
      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      currAvailableWorkerList.addAll(liveInstances);

      for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
        Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
        // TODO: one instance suppose to have only one partition
        for (TopicPartition tp : topicPartitions) {
          String pipeline = tp.getTopic();
          if (pipeline.startsWith(SEPARATOR)) {
            currRouteToInstanceMap.putIfAbsent(tp, new ArrayList<>());
            currRouteToInstanceMap.get(tp).add(instanceName);
            currAvailableWorkerList.remove(instanceName);
          }
        }
      }
      _routeToInstanceMap = currRouteToInstanceMap;
      _availableWorkerList = currAvailableWorkerList;
    } finally {
      _lock.unlock();
    }
    LOGGER.info("For worker _routeToInstanceMap: {}", _routeToInstanceMap);
    LOGGER.info("For worker {} available, _availableWorkerList: {}", _availableWorkerList.size(), _availableWorkerList);
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

  public synchronized void addTopicToMirrorMaker(InstanceTopicPartitionHolder controller, String pipeline, int routeId)
      throws Exception {
    updateCurrentStatus();
    LOGGER.info("Trying to create route pipeline: {}", pipeline);
    _lock.lock();
    try {
      if (_availableWorkerList.size() == 0) {
        LOGGER.info("No available worker!");
        throw new Exception("No available worker!");
      }
      List<String> instances = new ArrayList<>();
      for (int i = 0; i < _conf.getInitMaxNumWorkersPerRoute() && _availableWorkerList.size() >= 0; i++) {
        instances.add(_availableWorkerList.get(i));
      }
      if (!isPipelineExisted(pipeline)) {
        setEmptyResourceConfig(pipeline);
        _helixAdmin.addResource(_helixClusterName, pipeline,
            IdealStateBuilder.buildCustomIdealStateFor(pipeline, String.valueOf(routeId), instances));
      } else {
        _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
            IdealStateBuilder.expandCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                pipeline, String.valueOf(routeId), instances, _conf.getMaxNumWorkersPerRoute()));
      }
      TopicPartition route = new TopicPartition(pipeline, routeId);
      _routeToInstanceMap.putIfAbsent(route, new ArrayList<>());
      _routeToInstanceMap.get(route).addAll(instances);
      _availableWorkerList.removeAll(instances);
      controller.addWorkers(instances);
    } finally {
      _lock.unlock();
    }
  }

  public synchronized void deletePipelineInMirrorMaker(String pipeline) {
    _lock.lock();
    try {
      _helixAdmin.dropResource(_helixClusterName, pipeline);
      List<String> deletedInstances = new ArrayList<>();
      List<TopicPartition> tpToDelete = new ArrayList<>();
      for (TopicPartition tp : _routeToInstanceMap.keySet()) {
        if (tp.getTopic().equals(pipeline)) {
          deletedInstances.addAll(_routeToInstanceMap.get(tp));
          tpToDelete.add(tp);
        }
      }
      for (TopicPartition tp : tpToDelete) {
        _routeToInstanceMap.remove(tp);
      }
      _availableWorkerList.addAll(deletedInstances);
    } finally {
      _lock.unlock();
    }
  }

  public synchronized void replaceWorkerInMirrorMaker(Map<String, List<String>> pipelineToRouteIdToReplace, List<String> workerToReplace) {
    _lock.lock();
    try {
      LOGGER.info("replace: {}", _availableWorkerList);
      for (String pipeline : pipelineToRouteIdToReplace.keySet()) {
        LOGGER.info("replace: {} : {}", pipeline, pipelineToRouteIdToReplace.get(pipeline));
        // TODO: if there aren't enough workers
        _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
            IdealStateBuilder.resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                pipeline, workerToReplace, _availableWorkerList,
                _conf.getMaxNumWorkersPerRoute()));
      }
    } finally {
      _lock.unlock();
    }
  }

  private synchronized void setEmptyResourceConfig(String topicName) {
    _helixAdmin.setConfig(new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(_helixClusterName)
        .forResource(topicName).build(), new HashMap<>());
  }

  public List<String> getBlacklistedInstances() {
    return _helixAdmin.getInstancesInClusterWithTag(_helixClusterName, BLACKLIST_TAG);
  }

  public List<String> getAvailableWorkerList() {
    return _availableWorkerList;
  }

  public HelixManager getHelixManager() {
    return _helixManager;
  }

  public Map<TopicPartition, List<String>> getWorkerRouteToInstanceMap() {
    return _routeToInstanceMap;
  }

  public IuReplicatorConf getConf() {
    return _conf;
  }

}
