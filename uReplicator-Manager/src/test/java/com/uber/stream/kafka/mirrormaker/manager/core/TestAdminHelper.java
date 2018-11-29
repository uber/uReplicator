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
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.manager.utils.ControllerUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
public class TestAdminHelper {
    @Test
    public void testSetControllerAutobalancing() throws Exception {
        ControllerHelixManager mockHelixManager = EasyMock.createMock(ControllerHelixManager.class);
        AdminHelper helper = new AdminHelper(mockHelixManager);
        Map<String, PriorityQueue<InstanceTopicPartitionHolder>> instanceMap = new HashMap<>();
        PriorityQueue<InstanceTopicPartitionHolder> sjc1aTosjc1Agg1 = new PriorityQueue<>();
        String pipeline = ControllerUtils.getPipelineName("sjc1a", "sjc1-agg1");
        InstanceTopicPartitionHolder ith = new InstanceTopicPartitionHolder("compute9527-sjc1",
            new TopicPartition(pipeline, 0));
        sjc1aTosjc1Agg1.add(ith);
        instanceMap.put(pipeline, sjc1aTosjc1Agg1);
        EasyMock.expect(mockHelixManager.getPipelineToInstanceMap()).andReturn(instanceMap).atLeastOnce();
        EasyMock.expect(mockHelixManager.notifyControllerAutobalancing("compute9527-sjc1", true))
            .andReturn(true);
        EasyMock.replay(mockHelixManager);
        Map<String, Boolean> status = helper.setControllerAutobalancing("sjc1a", "sjc1-agg1", true);
        Assert.assertEquals(status.size(), 1);
        Assert.assertEquals(status.get("compute9527-sjc1"), true);
        EasyMock.verify(mockHelixManager);
    }
    @Test
    public void testGetControllerAutobalancing() throws Exception {
        ControllerHelixManager mockHelixManager = EasyMock.createMock(ControllerHelixManager.class);
        AdminHelper helper = new AdminHelper(mockHelixManager);
        Map<String, PriorityQueue<InstanceTopicPartitionHolder>> instanceMap = new HashMap<>();
        PriorityQueue<InstanceTopicPartitionHolder> sjc1aTosjc1Agg1 = new PriorityQueue<>();
        String pipeline = ControllerUtils.getPipelineName("sjc1a", "sjc1-agg1");
        InstanceTopicPartitionHolder ith = new InstanceTopicPartitionHolder("compute9527-sjc1",
            new TopicPartition(pipeline, 0));
        sjc1aTosjc1Agg1.add(ith);
        instanceMap.put(pipeline, sjc1aTosjc1Agg1);
        EasyMock.expect(mockHelixManager.getPipelineToInstanceMap()).andReturn(instanceMap).atLeastOnce();
        EasyMock.expect(mockHelixManager.getControllerAutobalancingStatus("compute9527-sjc1"))
            .andReturn(false);
        EasyMock.replay(mockHelixManager);
        JSONObject status = helper.getControllerAutobalancingStatus();
        Assert.assertEquals(status.size(), 1);
        JSONObject detail = (JSONObject) status.get("compute9527-sjc1");
        Assert.assertEquals(detail.get("autoBalance"), false);
        EasyMock.verify(mockHelixManager);
    }
}
