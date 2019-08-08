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

import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.worker.helix.ManagerWorkerHelixHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unittest to make sure helix handler working properly
 */
public class HelixHandlerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixHandlerTest.class);

  static WorkerInstance mockWorker = EasyMock.createMock(WorkerInstance.class);

  @BeforeMethod
  public void setup() {
    ZkStarter.startLocalZkServer();
    KafkaUReplicatorMetricsReporter.init(null);
  }

  @Test
  public void testManagerWorkerHelixInconsistentMessages() throws Exception {

    WorkerConf workerConf = TestUtils.initWorkerConf();
    Properties helixProps = WorkerUtils.loadAndValidateHelixProps(workerConf.getHelixConfigFile());
    // setup helix controller
    String route = String.format("%s-%s-0", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
    String routeForHelix = String.format("@%s@%s", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
    String rout2 = String.format("%s-%s-0", TestUtils.SRC_CLUSTER_2, TestUtils.DST_CLUSTER);
    String route2ForHelix = String.format("@%s@%s", TestUtils.SRC_CLUSTER_2, TestUtils.DST_CLUSTER);

    ZKHelixAdmin helixAdmin = TestUtils.initHelixClustersForWorkerTest(helixProps, route, rout2);
    String deployment = helixProps.getProperty("federated.deployment.name");
    String managerHelixClusterName = WorkerUtils.getManagerWorkerHelixClusterName(deployment);
    String instanceId = helixProps.getProperty("instanceId");

    EasyMock.reset(mockWorker);
    mockWorker.start(TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER, "0", deployment);
    EasyMock.expectLastCall().times(1);

    mockWorker.start(TestUtils.SRC_CLUSTER_2, TestUtils.DST_CLUSTER, "0", deployment);
    EasyMock.expectLastCall().times(1);

    mockWorker.cleanShutdown();
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(mockWorker.isRunning()).andReturn(false).anyTimes();

    EasyMock.replay(mockWorker);


    ManagerWorkerHelixHandler managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    managerWorkerHelixHandler.start();

    TestUtils
        .updateRouteWithValidation(managerHelixClusterName, routeForHelix, instanceId, helixAdmin,
            "ONLINE", "ONLINE");
    TestUtils
        .updateRouteWithValidation(managerHelixClusterName, route2ForHelix, instanceId, helixAdmin,
            "ONLINE", "ERROR");
    // mark offline to error not propagate to worker
    TestUtils
        .updateRouteWithValidation(managerHelixClusterName, route2ForHelix, instanceId, helixAdmin,
            "OFFLINE", "ERROR");

    managerWorkerHelixHandler.shutdown();

    // set the first route to offline
    IdealState idealState = TestUtils
        .buildManagerWorkerCustomIdealState(routeForHelix, Collections.singletonList(instanceId),
            "OFFLINE");
    helixAdmin.setResourceIdealState(managerHelixClusterName, routeForHelix, idealState);

    managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    managerWorkerHelixHandler.start();

    Thread.sleep(1000);

    ExternalView externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, route2ForHelix);
    Assert.assertNotNull(externalView);
    Assert.assertNotNull(externalView.getStateMap("0"));
    LOGGER.info("ExternalView: {}", externalView);
    Assert.assertEquals(externalView.getStateMap("0").get("0"), "OFFLINE");

    TestUtils
        .updateRouteWithValidation(managerHelixClusterName, route2ForHelix, instanceId, helixAdmin,
            "ONLINE", "ONLINE");
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

    EasyMock.expect(mockWorker.isRunning()).andReturn(false).anyTimes();

    mockWorker.start(TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER, "0", deployment);
    EasyMock.expectLastCall().andThrow(new RuntimeException("start mockWorker failed")).times(1);

    mockWorker.start(TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER, "0", deployment);
    EasyMock.expectLastCall().times(3);

    mockWorker.cleanShutdown();
    EasyMock.expectLastCall().anyTimes();


    mockWorker.addTopicPartition(mockTopic, 0);
    EasyMock.expectLastCall().andThrow(new RuntimeException("add topic partition failed")).times(2);

    mockWorker.addTopicPartition(mockTopic, 0);
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(mockWorker);

    ManagerWorkerHelixHandler managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    LOGGER.info("starting managerWorkerHelixHandler");
    managerWorkerHelixHandler.start();

    TestUtils
        .updateRouteWithValidation(managerHelixClusterName, routeForHelix, instanceId, helixAdmin,
            "ONLINE", "ERROR");

    LOGGER.info("shutting down managerWorkerHelixHandler");
    managerWorkerHelixHandler.shutdown();

    managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    LOGGER.info("starting managerWorkerHelixHandler again");
    managerWorkerHelixHandler.start();
    Thread.sleep(1000);

    ExternalView externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, routeForHelix);
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId), "ONLINE");

    TestUtils.updateTopicWithValidation(controllerHelixClusterName, mockTopic, Arrays.asList(0),
        Arrays.asList(instanceId),
        helixAdmin, "ONLINE", "ERROR");
    LOGGER.info("shutting down managerWorkerHelixHandler");

    managerWorkerHelixHandler.shutdown();

    // expected error since  mockWorker.addTopicPartition(mockTopic, 0) expect two error
    managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    LOGGER.info("starting managerWorkerHelixHandler again");
    managerWorkerHelixHandler.start();
    Thread.sleep(1000);
    externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, routeForHelix);
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId), "ONLINE");

    externalView = helixAdmin
        .getResourceExternalView(controllerHelixClusterName, mockTopic);

    Assert.assertNotNull(externalView);
    Assert.assertNotNull(externalView.getStateMap("0"));
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId),
        "ERROR");


    managerWorkerHelixHandler.shutdown();

    // expected error since mockWorker.addTopicPartition(mockTopic, 0) will success
    managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf,
        helixProps,
        mockWorker);
    LOGGER.info("starting managerWorkerHelixHandler again");
    managerWorkerHelixHandler.start();
    Thread.sleep(1000);
    externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, routeForHelix);
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId), "ONLINE");

    externalView = helixAdmin
        .getResourceExternalView(controllerHelixClusterName, mockTopic);

    // expected error since     mockWorker.addTopicPartition(mockTopic, 0) expect two error
    Assert.assertNotNull(externalView);
    Assert.assertNotNull(externalView.getStateMap("0"));
    Assert.assertEquals(externalView.getStateMap("0").get(instanceId),
        "ONLINE");
  }

  @AfterMethod
  public void shutdown() {
    ZkStarter.stopLocalZkServer();
  }
}
