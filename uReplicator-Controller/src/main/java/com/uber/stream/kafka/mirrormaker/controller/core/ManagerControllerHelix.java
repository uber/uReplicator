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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.ControllerInstance;
import com.uber.stream.kafka.mirrormaker.controller.utils.HelixUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helix between uReplicator Manager and Controllers.
 * @author Zhenmin Li
 */
public class ManagerControllerHelix {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManagerControllerHelix.class);

  private static final String MANAGER_CONTROLLER_HELIX_PREFIX = "manager-controller-";
  private static final String CONTROLLER_WORKER_HELIX_PREFIX = "controller-worker-";

  private final ControllerConf _controllerConf;
  private final String _helixClusterName;
  private final String _helixZkURL;
  private final String _instanceId;
  private HelixManager _helixZkManager;

  private final Object _handlerLock = new Object();

  private ControllerInstance _currentControllerInstance = null;
  private String _currentSrcCluster = null;
  private String _currentDstCluster = null;
  private String _currentRoutePartition = null;

  public ManagerControllerHelix(ControllerConf controllerConf) {
    _controllerConf = controllerConf;
    _helixClusterName = MANAGER_CONTROLLER_HELIX_PREFIX + _controllerConf.getDeploymentName();
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(_controllerConf.getZkStr());
    _instanceId = controllerConf.getInstanceId();
  }

  public synchronized void start() {
    LOGGER.info("Trying to start ManagerControllerHelix!");
    _helixZkManager = HelixManagerFactory.getZKHelixManager(_helixClusterName,
        _instanceId,
        InstanceType.PARTICIPANT,
        _helixZkURL);
    _helixZkManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new ControllerStateModelFactory(this));
    try {
      _helixZkManager.connect();
    } catch (Exception e) {
      LOGGER.error("Failed to start ManagerControllerHelix " + _helixClusterName, e);
    }
  }

  public synchronized void stop() {
    LOGGER.info("Trying to stop ManagerControllerHelix!");
    _helixZkManager.disconnect();
  }

  public void handleRouteAssignmentEvent(String srcCluster, String dstCluster, String routePartition, String toState) {
    synchronized (_handlerLock) {
      if (toState.equals("ONLINE")) {
        handleRouteAssignmentOnline(srcCluster, dstCluster, routePartition);
      } else if (toState.equals("OFFLINE")) {
        handleRouteAssignmentOffline(srcCluster, dstCluster, routePartition);
      } else if (toState.equals("DROPPED")) {
        handleRouteAssignmentDropped(srcCluster, dstCluster, routePartition);
      } else {
        LOGGER.error("Invalid route assignement state {}", toState);
      }
    }
  }

  private void handleRouteAssignmentOnline(String srcCluster, String dstCluster, String routePartition) {
    if (_currentControllerInstance != null) {
      if (!(srcCluster.equals(_currentSrcCluster) && dstCluster.equals(_currentDstCluster)
          && routePartition.equals(_currentRoutePartition))) {
        LOGGER.error(
            "Invalid route partition assignment. Current route src={}, dst={}, partition={}; new route src={}, dst={}, partition={}",
            _currentSrcCluster, _currentDstCluster, _currentRoutePartition, srcCluster, dstCluster,
            routePartition);
      } else {
        LOGGER.info("Controller has already been started");
      }
      return;
    }

    String clusterName = CONTROLLER_WORKER_HELIX_PREFIX + srcCluster + "-" + dstCluster + "-" + routePartition;
    _controllerConf.setHelixClusterName(clusterName);
    _controllerConf.setEnableSrcKafkaValidation("true");
    _controllerConf.setConsumerCommitZkPath(_controllerConf.getZkStr());
    _controllerConf.setGroupId(srcCluster + "-" + dstCluster);

    _currentControllerInstance = new ControllerInstance(this, _controllerConf);
    LOGGER.info("Starting controller instance for route {}", clusterName);
    try {
      _currentControllerInstance.start();
    } catch (Exception e) {
      LOGGER.error("Failed to start controller instance. Shutdown now.", e);
      System.exit(1);
    }
    _currentSrcCluster = srcCluster;
    _currentDstCluster = dstCluster;
    _currentRoutePartition = routePartition;
    LOGGER.info("Successfully started controller instance for route {}", clusterName);
  }

  private boolean handleRouteAssignmentOffline(String srcCluster, String dstCluster, String routePartition) {
    if (_currentControllerInstance == null) {
      LOGGER.error("Controller is not started yet");
      return false;
    }
    String clusterName = CONTROLLER_WORKER_HELIX_PREFIX + srcCluster + "-" + dstCluster + "-" + routePartition;
    if (!clusterName.equals(_currentRoutePartition)) {
      LOGGER.error("Invalid route partition assignment. Current route={}, the route to offline={}",
          _currentRoutePartition, clusterName);
      return false;
    }
    LOGGER.info("Stopping controller instance for cluster: " + clusterName);
    try {
      _currentControllerInstance.stop();
    } catch (Exception e) {
      LOGGER.error("Failed to stop controller instance. Shutdown now.", e);
      System.exit(-1);
    }
    _currentSrcCluster = null;
    _currentDstCluster = null;
    _currentRoutePartition = null;
    _currentControllerInstance = null;
    LOGGER.info("Successfully stopped controller instance for route {}", clusterName);
    return true;
  }

  private boolean handleRouteAssignmentDropped(String srcCluster, String dstCluster, String routePartition) {
    return handleRouteAssignmentOffline(srcCluster, dstCluster, routePartition);
  }

  public boolean handleTopicAssignmentEvent(String topic, String srcCluster, String dstCluster, String routePartition, String toState) {
    synchronized (_handlerLock) {
      if (_currentControllerInstance == null) {
        LOGGER.info(
            "Controller is not started yet. Start a new instance: srcCluster={}, dstCluster={}, routePartition={}",
            srcCluster, dstCluster, routePartition);
        handleRouteAssignmentOnline(srcCluster, dstCluster, routePartition);
      }
      if (!(srcCluster.equals(_currentSrcCluster) && dstCluster.equals(_currentDstCluster))) {
        LOGGER.error("Inconsistent route assignment: expected src={}, dst={}, but given src={}, dst={}",
            _currentSrcCluster, _currentDstCluster, srcCluster, dstCluster);
        return false;
      }
      if (toState.equals("ONLINE")) {
        return handleTopicAssignmentOnline(topic, srcCluster, dstCluster);
      } else if (toState.equals("OFFLINE")) {
        return handleTopicAssignmentOffline(topic, srcCluster, dstCluster);
      } else if (toState.equals("DROPPED")) {
        return handleTopicAssignmentDropped(topic, srcCluster, dstCluster);
      } else {
        LOGGER.error("Invalid topic assignement state: " + toState);
        return false;
      }
    }
  }

  private boolean handleTopicAssignmentOnline(String topic, String srcCluster, String dstCluster) {
    HelixMirrorMakerManager helixManager = _currentControllerInstance.getHelixResourceManager();
    if (helixManager.isTopicExisted(topic)) {
      LOGGER.warn("Topic {} already exists from cluster {} to {}", topic, srcCluster, dstCluster);
      return false;
    }
    KafkaBrokerTopicObserver topicObserver = _currentControllerInstance.getSourceKafkaTopicObserver();
    if (topicObserver == null) {
      LOGGER.error("Source broker observer is not initiated for cluster {}", srcCluster);
      return false;
    }
    TopicPartition topicPartitionInfo = topicObserver.getTopicPartitionWithRefresh(topic);
    if (topicPartitionInfo == null) {
      LOGGER.error(
          "Failed to whitelist topic {} on controller because topic does not exists in src cluster {}",
          topic, srcCluster);
      return false;
    }
    helixManager.addTopicToMirrorMaker(topicPartitionInfo);
    LOGGER.info("Whitelisted topic {} from cluster {} to {}", topic, srcCluster, dstCluster);
    return true;
  }

  private boolean handleTopicAssignmentOffline(String topic, String srcCluster, String dstCluster) {
    HelixMirrorMakerManager helixManager = _currentControllerInstance.getHelixResourceManager();
    if (!helixManager.isTopicExisted(topic)) {
      LOGGER.warn("Topic {} does not exist from cluster {} to {}", topic, srcCluster, dstCluster);
      return false;
    }
    helixManager.deleteTopicInMirrorMaker(topic);
    LOGGER.info("Blacklisted topic {} from {} to {}", topic, srcCluster, dstCluster);
    return true;
  }

  private boolean handleTopicAssignmentDropped(String topic, String srcCluster, String dstCluster) {
    return handleTopicAssignmentOffline(topic, srcCluster, dstCluster);
  }

}
