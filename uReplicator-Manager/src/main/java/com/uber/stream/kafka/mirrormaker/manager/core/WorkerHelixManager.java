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
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.model.IdealState;
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

  private final IuReplicatorConf _conf;
  private final String _helixZkURL;
  private final String _helixClusterName;
  private HelixManager _helixManager;
  private HelixAdmin _helixAdmin;
  private String _instanceId;

  private LiveInstanceChangeListener _liveInstanceChangeListener;

  public WorkerHelixManager(ManagerConf managerConf) {
    _conf = managerConf;
    _helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(managerConf.getManagerZkStr());
    _helixClusterName = MANAGER_WORKER_HELIX_PREFIX + "-" + managerConf.getManagerDeployment();
    _instanceId = managerConf.getManagerInstanceId();
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

  public IdealState getIdealStateForTopic(String topicName) {
    return _helixAdmin.getResourceIdealState(_helixClusterName, topicName);
  }

  public boolean isTopicExisted(String topicName) {
    return _helixAdmin.getResourcesInCluster(_helixClusterName).contains(topicName);
  }

  public List<String> getTopicLists() {
    return _helixAdmin.getResourcesInCluster(_helixClusterName);
  }

  public IuReplicatorConf getConf() {
    return _conf;
  }

}
