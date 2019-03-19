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
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.core.WorkloadInfoRetriever;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.common.modules.TopicPartitionLag;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;

import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;


public class TestHelixMirrorMakerManager {
  private HelixMirrorMakerManager helixMirrorMakerManager;
  private static WorkloadInfoRetriever workloadInfoRetriever = EasyMock.createMock(WorkloadInfoRetriever.class);
  private static OffsetMonitor offsetMonitor = EasyMock.createNiceMock(OffsetMonitor.class);
  private static String clusterName = "TestHelixMirrorMakerManager";
  private static List<String> fakeTopics = Arrays.asList("testTopic0", "testTopic1");
  private static TopicWorkload fakeWorkload1 = new TopicWorkload(1243.223, 32213, 2);
  private static TopicWorkload fakeWorkload2 = new TopicWorkload(21.1, 189, 2);
  private static TopicPartition fakeTopicPartition1 = new TopicPartition(fakeTopics.get(1), 0);
  private static TopicPartition fakeTopicPartition2 = new TopicPartition(fakeTopics.get(0), 1);

  @BeforeClass
  public void setup() {
    ControllerConf controllerConf = ControllerTestUtils.initControllerConf(clusterName);
    helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager._workloadInfoRetriever = workloadInfoRetriever;
    helixMirrorMakerManager._offsetMonitor = offsetMonitor;
  }

  @Test
  public void testCalculateLagTime() {
    EasyMock.expect(workloadInfoRetriever.topicWorkload(fakeTopics.get(0))).andReturn(fakeWorkload1).times(3);
    EasyMock.expect(workloadInfoRetriever.topicWorkload(fakeTopics.get(1))).andReturn(fakeWorkload2).times(2);
    EasyMock.expect(workloadInfoRetriever.isInitialized()).andReturn(true).anyTimes();

    EasyMock.expect(offsetMonitor.getTopicPartitionOffset(fakeTopicPartition1)).andReturn(
        new TopicPartitionLag(fakeTopicPartition1.getTopic(), fakeTopicPartition1.getPartition(), 12894, 2843));
    EasyMock.expect(offsetMonitor.getTopicPartitionOffset(fakeTopicPartition2)).andReturn(
        new TopicPartitionLag(fakeTopicPartition2.getTopic(), fakeTopicPartition2.getPartition(), 89600093, 2843));

    EasyMock.replay(workloadInfoRetriever, offsetMonitor);
    TopicPartitionLag lag = helixMirrorMakerManager.calculateLagTime(fakeTopicPartition1);
    Assert.assertNull(lag);

    TopicPartitionLag lag2 = helixMirrorMakerManager.calculateLagTime(fakeTopicPartition2);
    Assert.assertEquals(lag2.getLagTime(), 5563);
  }
}
