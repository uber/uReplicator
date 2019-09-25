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

import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @Author: Tboy
 */
public class ManagerWorkerSpectator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerWorkerSpectator.class);

    private static final String MANAGER_WORKER_HELIX_PREFIX  = "manager-worker";

    private final HelixManager spectator;

    private HelixAdmin helixAdmin;

    private final String clusterName;

    public ManagerWorkerSpectator(ControllerConf controllerConf){
        this.clusterName = MANAGER_WORKER_HELIX_PREFIX + "-" + controllerConf.getDeploymentName();
        this.spectator = HelixManagerFactory.getZKHelixManager(clusterName, controllerConf.getInstanceId(), InstanceType.SPECTATOR,
                HelixUtils.getAbsoluteZkPathForHelix(controllerConf.getZkStr()));
        System.out.println(this.clusterName);
    }

    public void start() {
        try {
            this.spectator.connect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.helixAdmin = this.spectator.getClusterManagmentTool();
        LOGGER.info("ManagerWorkerSpectator started");
    }

    public void stop(){
        this.helixAdmin.close();
        this.spectator.disconnect();
        LOGGER.info("ManagerWorkerSpectator stopped");
    }

    public Map<String, String> getInstanceStateMap(String pipeline, String routeId){
        IdealState resourceIdealState = this.helixAdmin.getResourceIdealState(clusterName, pipeline);
        Set<String> partitionSet = resourceIdealState.getPartitionSet();
        for(String partition : partitionSet){
            if(partition.equalsIgnoreCase(routeId)){
                Map<String, String> instanceStateMap = resourceIdealState.getInstanceStateMap(partition);
                return instanceStateMap;
            }
        }
        return Collections.emptyMap();
    }
}
