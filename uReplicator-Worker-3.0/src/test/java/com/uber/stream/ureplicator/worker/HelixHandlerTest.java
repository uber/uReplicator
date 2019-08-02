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
package com.uber.stream.ureplicator.worker;

import com.google.common.collect.ImmutableMap;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.worker.helix.ManagerWorkerHelixHandler;
import java.util.Collections;
import java.util.Properties;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Unittest to make sure helix handler working properly
 */
public class HelixHandlerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixHandlerTest.class);

  WorkerInstance mockWorker = EasyMock.createMock(WorkerInstance.class);

  @BeforeTest
  public void setup() {
    ZkStarter.startLocalZkServer();
    KafkaUReplicatorMetricsReporter.init(null);
  }

  @Test
  public void testHelixHandlerCatchesException() throws Exception {
    String mockTopic = "testHelixHandlerCatchesException";
    WorkerConf workerConf = TestUtils.initWorkerConf();
    Properties helixProps = WorkerUtils.loadAndValidateHelixProps(workerConf.getHelixConfigFile());
    // setup helix controller
    String route = String.format("%s-%s-0", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
    String routeForHelix = String.format("@%s@%s", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
    ZKHelixAdmin helixAdmin = TestUtils.initHelixClustersForWorkerTest(helixProps, route, null);
    String deployment = helixProps.getProperty("federated.deployment.name");
    String managerHelixClusterName = WorkerUtils.getManagerWorkerHelixClusterName(deployment);
    String controllerHelixClusterName = WorkerUtils.getControllerWorkerHelixClusterName(route);
    String instanceId = helixProps.getProperty("instanceId");

    EasyMock.reset(mockWorker);

    mockWorker.start(TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER, "0", deployment);
    EasyMock.expectLastCall().andThrow(new RuntimeException("start mockWorker failed")).times(1);

    mockWorker.start(TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER, "0", deployment);
    EasyMock.expectLastCall().times(2);

    mockWorker.cleanShutdown();
    EasyMock.expectLastCall().anyTimes();

    mockWorker.addTopicPartition(mockTopic, 0);
    EasyMock.expectLastCall().andThrow(new RuntimeException("add topic partition failed")).times(1);

    mockWorker.addTopicPartition(mockTopic, 0);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(mockWorker);

    ManagerWorkerHelixHandler managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    LOGGER.info("starting managerWorkerHelixHandler");
    managerWorkerHelixHandler.start();

    IdealState idealState = TestUtils
        .buildManagerWorkerCustomIdealState(routeForHelix, Collections.singletonList(instanceId),
            "ONLINE");
    helixAdmin.addResource(managerHelixClusterName, routeForHelix, idealState);
    Thread.sleep(1000);
    ExternalView externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, routeForHelix);
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId), "ERROR");

    LOGGER.info("shutting down managerWorkerHelixHandler");
    managerWorkerHelixHandler.shutdown();

    managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    LOGGER.info("starting managerWorkerHelixHandler again");
    managerWorkerHelixHandler.start();
    Thread.sleep(1000);

    externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, routeForHelix);
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId), "ONLINE");

    idealState = TestUtils.buildControllerWorkerCustomIdealState(mockTopic, ImmutableMap.of("0", instanceId), "ONLINE");
    helixAdmin.addResource(controllerHelixClusterName, mockTopic, idealState);
    Thread.sleep(1000);
     externalView = helixAdmin
        .getResourceExternalView(controllerHelixClusterName, mockTopic);
    LOGGER.info("{}", externalView);
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId), "ERROR");

    LOGGER.info("shutting down managerWorkerHelixHandler");
    managerWorkerHelixHandler.shutdown();

    managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    LOGGER.info("starting managerWorkerHelixHandler again");
    managerWorkerHelixHandler.start();
    Thread.sleep(1000);
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId), "ERROR");
  }

  @AfterTest
  public void shutdown() {
    ZkStarter.stopLocalZkServer();
  }
}
