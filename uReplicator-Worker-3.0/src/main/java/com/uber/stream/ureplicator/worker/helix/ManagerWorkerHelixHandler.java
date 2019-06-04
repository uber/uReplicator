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
package com.uber.stream.ureplicator.worker.helix;

import com.uber.stream.kafka.mirrormaker.common.core.HelixHandler;
import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateFactory;
import com.uber.stream.ureplicator.worker.Constants;
import com.uber.stream.ureplicator.worker.WorkerConf;
import com.uber.stream.ureplicator.worker.WorkerInstance;
import com.uber.stream.ureplicator.worker.WorkerStarter;
import com.uber.stream.ureplicator.worker.WorkerUtils;
import java.util.Date;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagerWorkerHelixHandler implements HelixHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerStarter.class);

  private final WorkerConf workerConf;
  private final String instanceId;
  private final String helixZkURL;
  private final String federatedDeploymentName;
  private final Properties helixProps;
  private final WorkerInstance workerInstance;
  private final HelixManager mangerWorkerHelixManager;
  private final String clusterName;

  private String currentSrcCluster;
  private String currentDstCluster;
  private String currentRouteId;
  private ControllerWorkerHelixHandler controllerWorkerHelixHandler;

  public ManagerWorkerHelixHandler(WorkerConf workerConf, Properties helixProps,
      WorkerInstance workerInstance) {
    if (!workerConf.getFederatedEnabled()) {
      throw new RuntimeException("ManagerWorkerHelixHandler only support federated mode");
    }
    this.workerConf = workerConf;
    this.helixProps = helixProps;
    this.helixZkURL = helixProps.getProperty(Constants.HELIX_ZK_SERVER,
        Constants.ZK_SERVER);
    this.instanceId = helixProps.getProperty(
        Constants.HELIX_INSTANCE_ID,
        "uReplicator-" + new Date().getTime());
    this.federatedDeploymentName = helixProps.getProperty(Constants.FEDERATED_DEPLOYMENT_NAME,
        null);
    this.clusterName = WorkerUtils.getManagerWorkerHelixClusterName(federatedDeploymentName);
    if (workerConf.getFederatedEnabled()) {
      if (StringUtils.isEmpty(federatedDeploymentName)) {
        LOGGER.error("{} is missing in helix properties for federated mode",
            Constants.FEDERATED_DEPLOYMENT_NAME);
        throw new IllegalArgumentException(Constants.FEDERATED_DEPLOYMENT_NAME
            + "is required on helix property for federated mode");
      }

      if (StringUtils.isEmpty(workerConf.getClusterConfigFile())) {
        LOGGER.error("cluster config file required for federated mode");
        throw new IllegalArgumentException("cluster config file required for federated mode");
      }
    }

    this.workerInstance = workerInstance;
    this.mangerWorkerHelixManager = HelixManagerFactory
        .getZKHelixManager(clusterName,
            instanceId,
            InstanceType.PARTICIPANT,
            helixZkURL);
  }

  public void start() throws Exception {
    LOGGER.info("Starting MangerWorkerHelixManager");
    try {
      mangerWorkerHelixManager.connect();
    } catch (Exception e) {
      LOGGER.error("Connecting to mangerWorkerHelixManager failed", e);
      throw e;
    }
    mangerWorkerHelixManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new OnlineOfflineStateFactory(this));
    InstanceConfig instanceConfig = new InstanceConfig(instanceId);
    instanceConfig.setHostName(workerConf.getHostname());
    instanceConfig.setInstanceEnabled(true);
    mangerWorkerHelixManager.getConfigAccessor()
        .setInstanceConfig(clusterName, instanceId,
            instanceConfig);
    LOGGER.info("Register MangerWorkerHelixManager finished");
  }

  public void shutdown() {
    LOGGER.info("Shutting down MangerWorkerHelixManager");
    if (controllerWorkerHelixHandler != null) {
      controllerWorkerHelixHandler.shutdown();
    }
    if (mangerWorkerHelixManager.isConnected()) {
      mangerWorkerHelixManager.disconnect();
    }
    LOGGER.info("Shutdown MangerWorkerHelixManager finished");

  }

  @Override
  public void onBecomeOnlineFromOffline(Message message) {
    handleRouteAssignmentOnline(message);
  }

  @Override
  public void onBecomeOfflineFromOnline(Message message) {
    handleRouteAssignmentOffline(message);
  }

  @Override
  public void onBecomeDroppedFromOffline(Message message) {
    handleRouteAssignmentOffline(message);
  }

  @Override
  public void onBecomeOfflineFromError(Message message) {
    handleRouteAssignmentOffline(message);
  }

  @Override
  public void onBecomeDroppedFromError(Message message) {
    handleRouteAssignmentOffline(message);
  }


  private synchronized void handleRouteAssignmentOnline(Message message) {
    String[] clustersInfo = WorkerUtils.parseSrcDstCluster(message.getResourceName());
    if (clustersInfo == null || clustersInfo.length != 3) {
      LOGGER.info("Invalid clusters: {}", clustersInfo);
      return;
    }
    String srcCluster = clustersInfo[1];
    String dstCluster = clustersInfo[2];
    String routeId = message.getPartitionName();

    if (!validateRequest(srcCluster, dstCluster, routeId)) {
      return;
    }

    if (controllerWorkerHelixHandler != null) {
      LOGGER.warn("Instance already online. srcCluster: {}, dstCluster: {}, routeId: {}",
          srcCluster, dstCluster, routeId);
      return;
    }

    LOGGER.info("Handling resource online {}", message.getResourceName());
    String routeName = String.format("%s-%s-%s", srcCluster, dstCluster, routeId);
    String helixCluster = WorkerUtils.getControllerWorkerHelixClusterName(routeName);
    LOGGER.error("Join controller-worker cluster {}", helixCluster);
    controllerWorkerHelixHandler = new ControllerWorkerHelixHandler(helixProps,
        helixCluster, srcCluster, dstCluster, routeId, federatedDeploymentName, workerInstance);
    try {
      controllerWorkerHelixHandler.start();
    } catch (Exception e) {
      LOGGER.error("Start controllerWorkerHelixHandler failed", e);
    }
    currentSrcCluster = srcCluster;
    currentDstCluster = dstCluster;
    LOGGER.info("Handling resource online {} finished", message.getResourceName());
  }

  private boolean validateRequest(String srcCluster, String dstCluster, String routeId) {
    if (srcCluster.equalsIgnoreCase(dstCluster)) {
      LOGGER.error("srcCluster and dstCluster is the same cluster : {}", srcCluster);
      return false;
    }

    if (StringUtils.isEmpty(currentSrcCluster) || StringUtils.isEmpty(currentDstCluster)) {
      return true;
    }

    if (srcCluster.equalsIgnoreCase(currentSrcCluster) &&
        dstCluster.equalsIgnoreCase(currentDstCluster) &&
        routeId.equalsIgnoreCase(currentRouteId)) {
      return true;
    } else {
      LOGGER.error("Inconsistent online request, srcCluster: {}, dstCluster: {}, routeId: {}.",
          srcCluster,
          dstCluster, routeId);
      return false;
    }
  }

  private synchronized void handleRouteAssignmentOffline(Message message) {
    if (controllerWorkerHelixHandler == null) {
      LOGGER.warn("Controller route not online yet.");
      return;
    }
    String[] clustersInfo = WorkerUtils.parseSrcDstCluster(message.getResourceName());
    if (clustersInfo == null || clustersInfo.length != 3) {
      return;
    }
    String srcCluster = clustersInfo[1];
    String dstCluster = clustersInfo[2];
    String routeId = message.getPartitionName();
    if (!validateRequest(srcCluster, dstCluster, routeId)) {
      return;
    }
    LOGGER.info("Handling resource offline {}", message.getResourceName());
    if (controllerWorkerHelixHandler != null) {
      controllerWorkerHelixHandler.shutdown();
      controllerWorkerHelixHandler = null;
      currentDstCluster = null;
      currentSrcCluster = null;
      currentRouteId = null;
    }
    LOGGER.info("Handling resource offline {} finished", message.getResourceName());
  }
}
