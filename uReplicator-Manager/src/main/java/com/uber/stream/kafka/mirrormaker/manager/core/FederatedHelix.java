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

import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.manager.utils.HelixUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main logic for Helix Manager-Controller and Manager-Worker.
 *
 * @author hongxu
 */
public class FederatedHelix {


  private static final Logger LOGGER = LoggerFactory.getLogger(FederatedHelix.class);

  private static final String MANAGER_CONTROLLER_HELIX_PREFIX = "manager-controller";
  private static final String MANAGER_WORKER_HELIX_PREFIX = "manager-worker";

  private final ManagerConf _managerConf;
  private final String _helixZkURL;
  private final String _controllerHelixClusterName;
  private final String _workerHelixClusterName;
  private HelixManager _controllerHelixManager;
  private HelixManager _workerHelixManager;
  private HelixAdmin _controllerHelixAdmin;
  private HelixAdmin _workerHelixAdmin;
  private String _instanceId;

  private ControllerLiveInstanceChangeListener _ControllerLiveInstanceChangeListener;
  private WorkerLiveInstanceChangeListener _WorkerLiveInstanceChangeListener;

  public FederatedHelix(ManagerConf managerConf) {
    _managerConf = managerConf;
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(_managerConf.getManagerZkStr());
    _controllerHelixClusterName = MANAGER_CONTROLLER_HELIX_PREFIX + "-" + _managerConf.getManagerDeployment();
    _workerHelixClusterName = MANAGER_WORKER_HELIX_PREFIX + "-" + _managerConf.getManagerDeployment();
    _instanceId = _managerConf.getManagerInstanceId();
  }

  public synchronized void start() {
    LOGGER.info("Trying to start ManagerControllerHelix!");
    _controllerHelixManager = HelixSetupUtils
        .setup(_controllerHelixClusterName, _helixZkURL, _instanceId);
    _controllerHelixAdmin = _controllerHelixManager.getClusterManagmentTool();

    LOGGER.info("Trying to register ControllerLiveInstanceChangeListener");
    _ControllerLiveInstanceChangeListener = new ControllerLiveInstanceChangeListener(this,
        _controllerHelixManager,
        _managerConf);
    try {
      _controllerHelixManager.addLiveInstanceChangeListener(_ControllerLiveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add ControllerLiveInstanceChangeListener");
    }

    LOGGER.info("Trying to start ManagerWorkerHelix!");
    _workerHelixManager = HelixSetupUtils.setup(_workerHelixClusterName, _helixZkURL, _instanceId);
    _workerHelixAdmin = _workerHelixManager.getClusterManagmentTool();

    LOGGER.info("Trying to register WorkerLiveInstanceChangeListener");
    _WorkerLiveInstanceChangeListener = new WorkerLiveInstanceChangeListener(this,
        _workerHelixManager,
        _managerConf);
    try {
      _workerHelixManager.addLiveInstanceChangeListener(_WorkerLiveInstanceChangeListener);
    } catch (Exception e) {
      LOGGER.error("Failed to add WorkerLiveInstanceChangeListener");
    }
  }

  public synchronized void stop() {
    LOGGER.info("Trying to stop ManagerControllerHelix!");
    _controllerHelixManager.disconnect();
    LOGGER.info("Trying to stop ManagerWorkerHelix!");
    _workerHelixManager.disconnect();
  }

}
