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

import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.utils.Constants;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.ControllerInstance;
import com.uber.stream.kafka.mirrormaker.controller.utils.HelixUtils;
import org.apache.helix.*;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
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

  private static final String CONFIG_KAFKA_CLUSTER_KEY_PREFIX = "kafka.cluster.zkStr.";

  private final ControllerConf _controllerConf;
  private final String _helixClusterName;
  private final String _helixZkURL;
  private final String _instanceId;
  private final String _hostname;
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
    _hostname = controllerConf.getHostname();
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
      ZkHelixPropertyStore<ZNRecord> propertyStore = _helixZkManager.getHelixPropertyStore();
      String resourcePath = Constants.PARTICIPATE_INSTANCE_ID_HOSTNAME_PROPERTY_KEY;
      ZNRecord znRecord = propertyStore.get(resourcePath, null, AccessOption.PERSISTENT);
      if (znRecord == null) {
        znRecord = new ZNRecord(resourcePath);
      }
      znRecord.setSimpleField(_instanceId, _hostname);
      propertyStore.set(resourcePath, znRecord, AccessOption.PERSISTENT);
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
        String msg = String.format(
            "Invalid route partition assignment. Current route src=%s, dst=%s, partition=%s; new route src=%s, dst=%s, partition=%s",
            _currentSrcCluster, _currentDstCluster, _currentRoutePartition, srcCluster, dstCluster, routePartition);
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
      } else {
        if (_currentControllerInstance.isStarted()) {
          LOGGER.info("Controller has already been started");
        } else {
          String msg = "Controller has already been initiated but not started yet";
          LOGGER.error(msg);
          throw new IllegalStateException(msg);
        }
      }
      return;
    }

    // validate src and dst clusters in configuration
    if (srcCluster.equals(dstCluster)) {
      String msg = String.format("The source cluster %s cannot be the same as destination cluster", srcCluster);
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }
    if (!_controllerConf.getSourceClusters().contains(srcCluster)) {
      String msg = String.format("The cluster %s is not a valid source cluster", srcCluster);
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }
    if (!_controllerConf.getDestinationClusters().contains(dstCluster)) {
      String msg = String.format("The cluster %s is not a valid destination cluster", dstCluster);
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }

    // set corresponding zkpath for src and dst clusters
    String srcKafkaZkPath = (String)_controllerConf.getProperty(CONFIG_KAFKA_CLUSTER_KEY_PREFIX + srcCluster);
    if (srcKafkaZkPath == null) {
      String msg = "Failed to find configuration of ZooKeeper path for source cluster " + srcCluster;
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }
    _controllerConf.setSrcKafkaZkPath(srcKafkaZkPath);
    String destKafkaZkPath = (String)_controllerConf.getProperty(CONFIG_KAFKA_CLUSTER_KEY_PREFIX + dstCluster);
    if (destKafkaZkPath == null) {
      String msg = "Failed to find configuration of ZooKeeper path for destination cluster " + dstCluster;
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }
    _controllerConf.setDestKafkaZkPath(destKafkaZkPath);

    String clusterName = CONTROLLER_WORKER_HELIX_PREFIX + srcCluster + "-" + dstCluster + "-" + routePartition;
    _controllerConf.setHelixClusterName(clusterName);
    _controllerConf.setEnableSrcKafkaValidation("true");
    _controllerConf.setGroupId("ureplicator-" + srcCluster + "-" + dstCluster);

    _currentControllerInstance = new ControllerInstance(this, _controllerConf);
    LOGGER.info("Starting controller instance for route {}", clusterName);
    try {
      _currentControllerInstance.start();
    } catch (Exception e) {
      String msg = "Failed to start controller instance. Roll back.";
      LOGGER.error(msg, e);
      if (_currentControllerInstance.stop()) {
        _currentControllerInstance = null;
      } else {
        LOGGER.error("Failed to stop the controller instance.");
      }
      throw new RuntimeException(msg);
    }
    _currentSrcCluster = srcCluster;
    _currentDstCluster = dstCluster;
    _currentRoutePartition = routePartition;
    LOGGER.info("Successfully started controller instance for route {}", clusterName);
  }

  private boolean handleRouteAssignmentOffline(String srcCluster, String dstCluster, String routePartition) {
    if (_currentControllerInstance == null) {
      String msg = "Controller instance is not started yet";
      LOGGER.info(msg);
      return false;
    }
    if (!(srcCluster.equals(_currentSrcCluster) && dstCluster.equals(_currentDstCluster)
        && routePartition.equals(_currentRoutePartition))) {
      String msg = String.format(
          "Invalid route to offline. Current route src=%s, dst=%s, routeId=%s; new route src=%s, dst=%s, routeId=%s",
          _currentSrcCluster, _currentDstCluster, _currentRoutePartition, srcCluster, dstCluster, routePartition);
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }
    String clusterName = CONTROLLER_WORKER_HELIX_PREFIX + srcCluster + "-" + dstCluster + "-" + routePartition;
    LOGGER.info("Stopping controller instance for cluster: " + clusterName);
    if (!_currentControllerInstance.stop()) {
      LOGGER.error("Failed to stop controller instance. Shutdown JVM instead.");
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
        if (toState.equals("OFFLINE") || toState.equals("DROPPED")) {
          LOGGER.error(
              "Controller is not started yet. Failed to action={} topic={} for srcCluster={}, dstCluster={}, routePartition={}",
              toState, topic, srcCluster, dstCluster, routePartition);
          return false;
        }
        LOGGER.info(
            "Controller is not started yet. Start a new instance: srcCluster={}, dstCluster={}, routePartition={}",
            srcCluster, dstCluster, routePartition);
        handleRouteAssignmentOnline(srcCluster, dstCluster, routePartition);
      }
      if (!(srcCluster.equals(_currentSrcCluster) && dstCluster.equals(_currentDstCluster))) {
        String msg = String.format("Inconsistent route assignment: expected src=%s, dst=%s, but given src=%s, dst=%s, toState=%s",
            _currentSrcCluster, _currentDstCluster, srcCluster, dstCluster, toState);
        LOGGER.error(msg);
        if (!toState.equals("OFFLINE") && !toState.equals("DROPPED")) {
          throw new IllegalArgumentException(msg);
        } else {
          return false;
        }
      }
      if (toState.equals("ONLINE")) {
        return handleTopicAssignmentOnline(topic, srcCluster, dstCluster);
      } else if (toState.equals("OFFLINE")) {
        return handleTopicAssignmentOffline(topic, srcCluster, dstCluster);
      } else if (toState.equals("DROPPED")) {
        return handleTopicAssignmentDropped(topic, srcCluster, dstCluster);
      } else {
        String msg = "Invalid topic assignement state: " + toState;
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
      }
    }
  }

  private boolean handleTopicAssignmentOnline(String topic, String srcCluster, String dstCluster) {
    HelixMirrorMakerManager helixManager = _currentControllerInstance.getHelixResourceManager();
    if (helixManager.isTopicExisted(topic)) {
      LOGGER.warn("Topic {} already exists from cluster {} to {}", topic, srcCluster, dstCluster);
      return false;
    }
    TopicPartition topicPartitionInfo = null;
    KafkaBrokerTopicObserver topicObserver = _currentControllerInstance.getSourceKafkaTopicObserver();
    if (topicObserver == null) {
      // no source partition information, use partitions=1 and depend on auto-expanding later
      topicPartitionInfo = new TopicPartition(topic, 1);
    } else {
      topicPartitionInfo = topicObserver.getTopicPartitionWithRefresh(topic);
      if (topicPartitionInfo == null) {
        String msg = String.format(
            "Failed to whitelist topic %s on controller because topic does not exists in src cluster %s",
            topic, srcCluster);
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
      }
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

  public ControllerInstance getControllerInstance() {
    return _currentControllerInstance;
  }

}
