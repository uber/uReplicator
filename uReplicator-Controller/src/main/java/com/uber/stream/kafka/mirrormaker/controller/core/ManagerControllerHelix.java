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
import com.uber.stream.kafka.mirrormaker.controller.utils.HelixUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.examples.OnlineOfflineStateModelFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helix between uReplicator Manager and Controllers.
 * @author Zhenmin Li
 */
public class ManagerControllerHelix {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManagerControllerHelix.class);

  private static final String HELIX_PREFIX = "manager-controller-";

  private final ControllerConf _controllerConf;
  private final String _helixClusterName;
  private final String _helixZkURL;
  private final String _instanceId;
  private HelixManager _helixZkManager;

  public ManagerControllerHelix(ControllerConf controllerConf) {
    _controllerConf = controllerConf;
    _helixClusterName = HELIX_PREFIX + _controllerConf.getDeploymentName();
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(_controllerConf.getZkStr());
    _instanceId = controllerConf.getInstanceId();
  }

  public synchronized void start() {
    LOGGER.info("Trying to start ManagerControllerHelix!");
    _helixZkManager = HelixManagerFactory.getZKHelixManager(_helixClusterName,
        _instanceId,
        InstanceType.PARTICIPANT,
        _helixZkURL);
    StateMachineEngine stateMach = _helixZkManager.getStateMachineEngine();
    stateMach.registerStateModelFactory("OnlineOffline", new OnlineOfflineStateModelFactory());
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

}
