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

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.manager.utils.ControllerUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * ALl the code related to Admin tasks can go here
 */
public class AdminHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminHelper.class);

    private ControllerHelixManager helixManager;
    public AdminHelper(ControllerHelixManager helixManager) {
        this.helixManager = helixManager;
    }
    public Map<String, Boolean> setControllerAutobalancing(boolean enable) {
        return setControllerAutobalancing("", "", enable);
    }

    public JSONObject getControllerAutobalancingStatus() {
        return getControllerAutobalancingStatus("", "");
    }

    public JSONObject getControllerAutobalancingStatus(String srcCluster, String dstCluster) {
        JSONObject retVal = new JSONObject();
        String pipelineFilter = "";
        if (StringUtils.isNotEmpty(srcCluster) && StringUtils.isNotEmpty(dstCluster)) {
            pipelineFilter = ControllerUtils.getPipelineName(srcCluster, dstCluster);
        }
        Map<String, PriorityQueue<InstanceTopicPartitionHolder>> map = helixManager.getPipelineToInstanceMap();
        for(String pipeline : map.keySet()) {
            if (pipelineFilter.isEmpty() || pipelineFilter.equals(pipeline)) {
                PriorityQueue<InstanceTopicPartitionHolder> routes =
                    helixManager.getPipelineToInstanceMap().get(pipeline);
                if (routes != null) {
                    for (InstanceTopicPartitionHolder route : routes) {
                        String instanceName = route.getInstanceName();
                        JSONObject status = new JSONObject();
                        status.put("pipeline", pipeline);
                        status.put("hostname", instanceName); //TODO: merge with instanceID change
                        try {
                            if (helixManager.getControllerAutobalancingStatus(instanceName)) {
                                status.put("autoBalance", true);
                                retVal.put(instanceName, status);
                            } else {
                                LOGGER.warn("Failed to notify {} but we will continue");
                                status.put("autoBalance", false);
                                retVal.put(instanceName, status);
                            }
                        } catch (ControllerException ex) {
                            LOGGER.warn("Failed to notify {} but we will continue");
                            retVal.put(instanceName, false);
                        }
                    }
                }
            }
        }
        return retVal;
    }
    /**
     * Notify controller (RPC call) to set controller autobalancing status
     *
     * @param srcCluster can be null or empty to target all
     * @param dstCluster can be null or empty to target all
     * @param enable set to true to enable, false to disable
     * @return a map showing the current instance status whether the autobalancing is enabled
     */
    public Map<String, Boolean> setControllerAutobalancing(String srcCluster, String dstCluster, boolean enable) {
        HashMap<String, Boolean> retVal = new HashMap<>();
        String pipelineFilter = "";
        if (StringUtils.isNotEmpty(srcCluster) && StringUtils.isNotEmpty(dstCluster)) {
            pipelineFilter = ControllerUtils.getPipelineName(srcCluster, dstCluster);
        }
        Map<String, PriorityQueue<InstanceTopicPartitionHolder>> map = helixManager.getPipelineToInstanceMap();
        for(String pipeline : map.keySet()) {
            if (pipelineFilter.isEmpty() || pipelineFilter.equals(pipeline)) {
                PriorityQueue<InstanceTopicPartitionHolder> routes =
                    helixManager.getPipelineToInstanceMap().get(pipeline);
                if (routes != null) {
                    for (InstanceTopicPartitionHolder route : routes) {
                        String instanceName = route.getInstanceName();
                        try {
                            if (helixManager.notifyControllerAutobalancing(instanceName, enable)) {
                                retVal.put(instanceName, true);
                            } else {
                                LOGGER.warn("Failed to notify {} but we will continue");
                                retVal.put(instanceName, false);
                            }
                        } catch (ControllerException ex) {
                            LOGGER.warn("Failed to notify {} but we will continue");
                            retVal.put(instanceName, false);
                        }
                    }
                }
            }
        }
        return retVal;
    }
}
