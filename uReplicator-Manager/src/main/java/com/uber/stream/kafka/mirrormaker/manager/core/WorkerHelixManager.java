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

import com.uber.stream.kafka.mirrormaker.common.configuration.IuReplicatorConf;
import com.uber.stream.kafka.mirrormaker.common.core.IHelixManager;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
      LOGGER.debug("For worker instanceToTopicPartitionsMap: {}", instanceToTopicPartitionsMap);
      List<String> liveInstances = HelixUtils.liveInstances(_helixManager);
      currAvailableWorkerList.addAll(liveInstances);

      Map<String, List<TopicPartition>> incorrectInstanceToTopicPartitionsMap = new HashMap<>();
      for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
        Set<TopicPartition> topicPartitions = instanceToTopicPartitionsMap.get(instanceName);
        // TODO: one instance suppose to have only one partition
        for (TopicPartition tp : topicPartitions) {
          String pipeline = tp.getTopic();
          if (pipeline.startsWith(SEPARATOR)) {
            currRouteToInstanceMap.putIfAbsent(tp, new ArrayList<>());
            currRouteToInstanceMap.get(tp).add(instanceName);
            if (!currAvailableWorkerList.contains(instanceName)) {
              LOGGER.info("not contain {} in {}@{}", instanceName, tp.getTopic(), tp.getPartition());
            }
            currAvailableWorkerList.remove(instanceName);
          } else {
            incorrectInstanceToTopicPartitionsMap.putIfAbsent(instanceName, new ArrayList<>());
            incorrectInstanceToTopicPartitionsMap.get(instanceName).add(tp);
          }
        }
      }
      _routeToInstanceMap = currRouteToInstanceMap;
      _availableWorkerList = currAvailableWorkerList;
      if (!incorrectInstanceToTopicPartitionsMap.isEmpty()) {
        LOGGER.error("Validate WRONG: wrong incorrectInstanceToTopicPartitionsMap: {}", incorrectInstanceToTopicPartitionsMap);
      }
    } finally {
      _lock.unlock();
    }

    //LOGGER.info("For worker _routeToInstanceMap: {}", _routeToInstanceMap);
    //LOGGER.info("For worker {} available, _availableWorkerList: {}", _availableWorkerList.size(), _availableWorkerList);
    LOGGER.info("For worker {} available", _availableWorkerList.size());
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
      for (int i = 0; i < _conf.getInitMaxNumWorkersPerRoute() && i < _availableWorkerList.size(); i++) {
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

  public synchronized void addWorkersToMirrorMaker(InstanceTopicPartitionHolder controller, String pipeline,
      int routeId, int numWorkersToAdd) throws Exception {
    LOGGER.info("Trying to add {} workers to route: {}@{}", numWorkersToAdd, pipeline, routeId);
    _lock.lock();
    try {
      if (_availableWorkerList.size() == 0) {
          LOGGER.warn("No available worker!");
          return;
      }
      List<String> instances = new ArrayList<>();
      for (int i = 0; i < numWorkersToAdd && i < _availableWorkerList.size(); i++) {
        instances.add(_availableWorkerList.get(i));
      }

      LOGGER.info("Add {} instance to route {}: {}", instances.size(), pipeline + SEPARATOR + routeId, instances);
      if (_helixAdmin.getResourceIdealState(_helixClusterName, pipeline) == null) {
        // this can happen when manager-controller idealstate is finished but manager-worker idealstate not exist
        if (!isPipelineExisted(pipeline)) {
          setEmptyResourceConfig(pipeline);
          _helixAdmin.addResource(_helixClusterName, pipeline,
              IdealStateBuilder.buildCustomIdealStateFor(pipeline, String.valueOf(routeId), instances));
        } else {
          _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
              IdealStateBuilder.expandCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                  pipeline, String.valueOf(routeId), instances, _conf.getMaxNumWorkersPerRoute()));
        }
      } else if (_helixAdmin.getResourceIdealState(_helixClusterName, pipeline).getPartitionSet().contains(String.valueOf(routeId))) {
        LOGGER.info("Topic {} Partition {} exists", pipeline, routeId);
        _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
            IdealStateBuilder.expandInstanceCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                    pipeline, String.valueOf(routeId), instances, _conf.getMaxNumWorkersPerRoute()));
      } else {
        LOGGER.info("Topic {} Partition {} does not exist", pipeline, routeId);
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

  public synchronized void removeWorkersToMirrorMaker(InstanceTopicPartitionHolder controller, String pipeline,
      int routeId, int numWorkersToRemove) throws Exception {
    LOGGER.info("Trying to remove {} workers in route: {}@{}", numWorkersToRemove, pipeline, routeId);
    _lock.lock();
    try {

      List<String> instancesToRemove = new ArrayList<>();
      Iterator<String> currWorkerIter = controller.getWorkerSet().iterator();
      int count = 0;
      while (count < numWorkersToRemove && currWorkerIter.hasNext()) {
        instancesToRemove.add(currWorkerIter.next());
        count++;
      }

      LOGGER.info("Remove {} instance int route {}: {}", instancesToRemove.size(), pipeline + SEPARATOR + routeId,
          instancesToRemove);
      _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
          IdealStateBuilder.shrinkInstanceCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                  pipeline, String.valueOf(routeId), instancesToRemove, _conf.getMaxNumWorkersPerRoute()));

      TopicPartition route = new TopicPartition(pipeline, routeId);
      _routeToInstanceMap.putIfAbsent(route, new ArrayList<>());
      _routeToInstanceMap.get(route).removeAll(instancesToRemove);
      _availableWorkerList.addAll(instancesToRemove);
      controller.removeWorkers(instancesToRemove);
    } finally {
      _lock.unlock();
    }
  }

  public synchronized void replaceWorkerInMirrorMaker(Map<String, List<String>> pipelineToRouteIdToReplace,
      List<String> workerToReplace) {
    _lock.lock();
    try {
      LOGGER.info("_availableWorkerList: {}", _availableWorkerList);
      for (String pipeline : pipelineToRouteIdToReplace.keySet()) {
        if(!_availableWorkerList.isEmpty()){
          LOGGER.info("replacing pipeline: {} : routeId : {}", pipeline, pipelineToRouteIdToReplace.get(pipeline));
          String workerInstance = _availableWorkerList.remove(0);
          _helixAdmin.setResourceIdealState(_helixClusterName, pipeline,
                  IdealStateBuilder.resetCustomIdealStateFor(_helixAdmin.getResourceIdealState(_helixClusterName, pipeline),
                          pipeline, workerToReplace, workerInstance,
                          _conf.getMaxNumWorkersPerRoute()));

        } else {
          LOGGER.error("no available worker to replace pipeline: {} : routeId : {}", pipeline, pipelineToRouteIdToReplace.get(pipeline));
          break;
        }
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
