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
package com.uber.stream.kafka.mirrormaker.controller.rest;

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.core.WorkloadInfoRetriever;
import com.uber.stream.kafka.mirrormaker.common.modules.ControllerWorkloadInfo;
import com.uber.stream.kafka.mirrormaker.common.modules.TopicPartitionLag;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.OffsetMonitor;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerRequestURLBuilder;
import org.easymock.EasyMock;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Protocol;
import org.restlet.data.Status;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

public class AdminRestletResourceTest {
  private final String CONTROLLER_PORT = "9999";
  private final String REQUEST_URL = "http://localhost:" + CONTROLLER_PORT;

  private final Component _component = new Component();
  private final Context applicationContext = _component.getContext().createChildContext();
  private final HelixMirrorMakerManager _helixMirrorMakerManager = EasyMock.createNiceMock(HelixMirrorMakerManager.class);
  private final ControllerRestApplication _controllerRestApp = new ControllerRestApplication(null);
  private static WorkloadInfoRetriever _workloadInfoRetriever = EasyMock.createMock(WorkloadInfoRetriever.class);
  private static OffsetMonitor _offsetMonitor = EasyMock.createNiceMock(OffsetMonitor.class);
  private static List<String> fakeTopics = Arrays.asList("testTopic0", "testTopic1");
  private static TopicWorkload fakeWorkload1 = new TopicWorkload(1243.223, 32213, 2);
  private static TopicWorkload fakeWorkload2 = new TopicWorkload(21.1, 189, 2);
  private static final PriorityQueue<InstanceTopicPartitionHolder> fakeItphList = new PriorityQueue<>(InstanceTopicPartitionHolder.perPartitionWorkloadComparator(null, null));

  private static final TopicPartition faketp1 = new TopicPartition(fakeTopics.get(0), 1);
  private static final TopicPartition faketp2 = new TopicPartition(fakeTopics.get(1), 0);
  private static final TopicPartition faketp3 = new TopicPartition(fakeTopics.get(0), 0);
  private static final TopicPartition faketp4 = new TopicPartition(fakeTopics.get(1), 1);
  private static final TopicPartitionLag faketplag = new TopicPartitionLag(faketp3.getTopic(), faketp3.getPartition(), 87428637, 22321);

  @BeforeTest
  public void setup() throws Exception {
    InstanceTopicPartitionHolder fakeInstance1 = new InstanceTopicPartitionHolder("0");
    InstanceTopicPartitionHolder fakeInstance2 = new InstanceTopicPartitionHolder("2");
    fakeInstance1.addTopicPartition(faketp1);
    fakeInstance1.addTopicPartition(faketp2);
    fakeInstance2.addTopicPartition(faketp3);
    fakeInstance2.addTopicPartition(faketp4);
    fakeItphList.add(fakeInstance1);
    fakeItphList.add(fakeInstance2);

    applicationContext.getAttributes().put(HelixMirrorMakerManager.class.toString(),
        _helixMirrorMakerManager);
    _controllerRestApp.setContext(applicationContext);
    _component.getServers().add(Protocol.HTTP, Integer.parseInt(CONTROLLER_PORT));
    _component.getClients().add(Protocol.FILE);
    _component.getDefaultHost().attach(_controllerRestApp);
    _component.start();

  }

  @Test
  public void testGetControllerWorkload() {
    EasyMock.reset(_helixMirrorMakerManager);
    EasyMock.expect(_helixMirrorMakerManager.getCurrentServingInstance()).andReturn(fakeItphList);
    EasyMock.expect(_helixMirrorMakerManager.getWorkloadInfoRetriever()).andReturn(_workloadInfoRetriever).anyTimes();

    EasyMock.expect(_workloadInfoRetriever.isInitialized()).andReturn(true);
    EasyMock.expect(_workloadInfoRetriever.topicWorkload(fakeTopics.get(0))).andReturn(fakeWorkload1).times(2);
    EasyMock.expect(_workloadInfoRetriever.topicWorkload(fakeTopics.get(1))).andReturn(fakeWorkload2).times(2);

    EasyMock.expect(_helixMirrorMakerManager.calculateLagTime(faketp1)).andReturn(null);
    EasyMock.expect(_helixMirrorMakerManager.calculateLagTime(faketp2)).andReturn(null);
    EasyMock.expect(_helixMirrorMakerManager.calculateLagTime(faketp3)).andReturn(null);
    faketplag.setLagTime(1243L);
    EasyMock.expect(_helixMirrorMakerManager.calculateLagTime(faketp4)).andReturn(faketplag);

    EasyMock.expect(_helixMirrorMakerManager.getMaxDedicatedInstancesRatio()).andReturn(0.2);
    EasyMock.expect(_helixMirrorMakerManager.getMaxWorkloadPerWorkerBytes()).andReturn(700.0);


    EasyMock.replay(_helixMirrorMakerManager, _workloadInfoRetriever);


    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getWorkloadInfoUrl();
    Response response = RestTestBase.HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);

    ControllerWorkloadInfo controllerWorkloadInfo = JSONObject.parseObject(response.getEntityAsText(), ControllerWorkloadInfo.class);
    Assert.assertEquals((int) controllerWorkloadInfo.getTopicWorkload().getBytesPerSecond(), 1264);

    Assert.assertEquals(controllerWorkloadInfo.getWorkerInstances().size(), 2);
    Assert.assertEquals(controllerWorkloadInfo.getNumOfLaggingWorkers(), 1);
    Assert.assertEquals(controllerWorkloadInfo.getNumOfExpectedWorkers(), 3);
  }

  @AfterTest
  public void cleanup() throws Exception {
    _controllerRestApp.stop();
    _component.stop();
  }
}
