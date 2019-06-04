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

import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class WorkerInstanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerInstanceTest.class);

  private KafkaServerStartable srcKafka1;
  private KafkaServerStartable srcKafka2;

  private KafkaServerStartable dstKafka;

  private final int srcClusterPort1 = 19092;
  private final int srcClusterPort2 = 19094;
  private final int dstClusterPort = 19093;

  private final String srcBootstrapServer = String.format("localhost:%d", srcClusterPort1);
  private final String dstBootstrapServer = String.format("localhost:%d", dstClusterPort);
  private final String srcClusterZK = ZkStarter.DEFAULT_ZK_STR + "/" + TestUtils.SRC_CLUSTER;
  private final String dstClusterZK = ZkStarter.DEFAULT_ZK_STR + "/" + TestUtils.DST_CLUSTER;

  private int numberOfPartitions = 2;

  @BeforeTest
  public void setup() throws Exception {
    Properties defaultConfig = KafkaStarterUtils.getDefaultKafkaConfiguration();
    defaultConfig.setProperty("offsets.topic.replication.factor", "2");

    ZkStarter.startLocalZkServer();
    Thread.sleep(1000);
    srcKafka1 = KafkaStarterUtils.startServer(srcClusterPort1,
        0,
        srcClusterZK,
        defaultConfig);
    srcKafka1.startup();

    srcKafka2 = KafkaStarterUtils.startServer(srcClusterPort2,
        1,
        srcClusterZK,
        defaultConfig);
    srcKafka2.startup();

    dstKafka = KafkaStarterUtils.startServer(dstClusterPort,
        KafkaStarterUtils.DEFAULT_BROKER_ID,
        dstClusterZK,
        KafkaStarterUtils.getDefaultKafkaConfiguration());

    dstKafka.startup();
  }

  @Test
  public void testNonFederatedWorkerInstance() throws InterruptedException {
    WorkerInstance workerInstance;
    WorkerConf conf = TestUtils.initWorkerConf();
    conf.setFederatedEnabled(false);
    workerInstance = new WorkerInstance(conf);
    try {
      String topicName1 = "testNonFederatedWorkerInstance1";
      String topicName2 = "testNonFederatedWorkerInstance2";
      KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, srcClusterZK, "2");
      KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, srcClusterZK, "2");
      KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, dstClusterZK, "1");
      KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, dstClusterZK, "1");
      workerInstance.start(null, null, null, null);

      workerInstance.addTopicPartition(topicName1, 0);
      workerInstance.addTopicPartition(topicName1, 1);

      LOGGER.info("Add topic partition finished");
      Thread.sleep(1000);

      TestUtils.produceMessages(srcBootstrapServer, topicName1, 20, 2);
      TestUtils.produceMessages(srcBootstrapServer, topicName2, 20, 2);
      LOGGER.info("Produce messages finished");

      List<ConsumerRecord<Byte[], Byte[]>> records = TestUtils
          .consumeMessage(dstBootstrapServer, topicName1, 5000);
      Assert.assertEquals(records.size(), 20);
      LOGGER.info("Shutdown testNonFederatedWorkerInstance");
      workerInstance.cleanShutdown();
      workerInstance = null;

      workerInstance = new WorkerInstance(conf);
      workerInstance.start(null, null, null,null);

      workerInstance.addTopicPartition(topicName1, 0);
      workerInstance.addTopicPartition(topicName1, 1);
      workerInstance.addTopicPartition(topicName2, 0, 0L, null, null);
      workerInstance.addTopicPartition(topicName2, 1, 0L, 5L, null);

      Thread.sleep(1000);
      TestUtils.produceMessages(srcBootstrapServer, topicName1, 20, 2);
      TestUtils.produceMessages(srcBootstrapServer, topicName2, 20, 2);

      records = TestUtils.consumeMessage(dstBootstrapServer, topicName1, 4000);

      Assert.assertEquals(records.size(), 20);

      records = TestUtils.consumeMessage(dstBootstrapServer, topicName2, 4000);
      Assert.assertEquals(records.size(), 25);

      LOGGER.info("Shutdown worker instance");
      workerInstance.cleanShutdown();
      Assert.assertEquals(true, workerInstance.isShuttingDown.get());
    } finally {
      workerInstance.cleanShutdown();
    }

  }

  public class WorkerStarterRunnable implements Runnable {

    private final WorkerStarter starter;

    public WorkerStarterRunnable(WorkerConf workerConf) {
      starter = new WorkerStarter(workerConf);
    }

    @Override
    public void run() {
      try {
        LOGGER.info("Starting WorkerStarter");
        starter.run();
      } catch (Exception e) {
        LOGGER.error("WorkerStarter failed", e);
      }
    }

    public void shutdown() {
      starter.shutdown();
    }
  }

  @Test
  public void testFederatedWorkerEndToEnd() throws Exception {
    String topicName1 = "tessFederatedWorkerEndToEnd1";
    String topicName2 = "tessFederatedWorkerEndToEnd2";
    KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, srcClusterZK, "2");
    KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, srcClusterZK, "2");
    KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, dstClusterZK, "1");
    KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, dstClusterZK, "1");
    ZkClient commitZkClient = ZkUtils.createZkClient(
        "localhost:12026/cluster1",
        10000,
        10000);
    WorkerConf workerConf = TestUtils.initWorkerConf();
    Properties helixProps = WorkerUtils.loadAndValidateHelixProps(workerConf.getHelixConfigFile());
    String deployment = "integration-test";
    String zkRoot = "localhost:12026/ureplicator";
    Thread.sleep(500);
    ZkClient zkClient = ZkUtils.createZkClient(ZkStarter.DEFAULT_ZK_STR, 1000, 1000);
    zkClient.createPersistent("/ureplicator");
    zkClient.close();

    String instanceId = helixProps.getProperty(Constants.HELIX_INSTANCE_ID, null);
    Assert.assertNotNull(instanceId, String
        .format("failed to find property %s in configuration file %s", Constants.HELIX_INSTANCE_ID,
            workerConf.getHelixConfigFile()));

    ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkRoot);
    String route = String.format("%s-%s-0", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
    String routeForHelix = String.format("@%s@%s", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);

    String managerHelixClusterName = WorkerUtils.getManagerWorkerHelixClusterName(deployment);
    String controllerHelixClusterName = WorkerUtils.getControllerWorkerHelixClusterName(route);
    HelixManager managerWorkerHelix = HelixSetupUtils
        .setup(managerHelixClusterName, zkRoot, "0");
    managerWorkerHelix.connect();
    HelixManager controllerWorkerHelix = HelixSetupUtils
        .setup(controllerHelixClusterName, zkRoot, "0");
    controllerWorkerHelix.connect();

    Thread.sleep(1000);
    WorkerStarterRunnable runnable = new WorkerStarterRunnable(workerConf);
    Thread workerThread = new Thread(runnable);
    workerThread.start();

    IdealState idealState = TestUtils
        .buildManagerWorkerCustomIdealState(routeForHelix, Collections.singletonList(instanceId),
            "ONLINE");
    helixAdmin.addResource(managerHelixClusterName, routeForHelix, idealState);
    LOGGER.info("add resource {} for cluster {} finished", routeForHelix, managerHelixClusterName);
    Thread.sleep(1000);
    ExternalView externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, routeForHelix);
    Assert.assertEquals(externalView.getStateMap("0").get("0"), "ONLINE");

    Map<String, String> partitionInstanceMap = new HashMap<>();
    partitionInstanceMap.put("0", "0");
    partitionInstanceMap.put("1", "0");
    idealState = TestUtils
        .buildControllerWorkerCustomIdealState(topicName1, partitionInstanceMap, "ONLINE");
    helixAdmin.addResource(controllerHelixClusterName, topicName1, idealState);
    Thread.sleep(1000);
    externalView = helixAdmin.getResourceExternalView(controllerHelixClusterName, topicName1);
    Assert.assertEquals(externalView.getStateMap("0").get("0"), "ONLINE");
    Assert.assertEquals(externalView.getStateMap("1").get("0"), "ONLINE");

    TestUtils.produceMessages(srcBootstrapServer, topicName1, 20);
    List<ConsumerRecord<Byte[], Byte[]>> records = TestUtils
        .consumeMessage(dstBootstrapServer, topicName1, 5000);
    Assert.assertEquals(records.size(), 20);

    idealState = TestUtils
        .buildControllerWorkerCustomIdealState(topicName1, partitionInstanceMap, "OFFLINE");
    helixAdmin.setResourceIdealState(controllerHelixClusterName, topicName1, idealState);
    Thread.sleep(2000);
    externalView = helixAdmin.getResourceExternalView(controllerHelixClusterName, topicName1);
    Assert.assertEquals(externalView.getStateMap("0").get("0"), "OFFLINE");
    Assert.assertEquals(externalView.getStateMap("1").get("0"), "OFFLINE");

    TestUtils.produceMessages(srcBootstrapServer, topicName1, 20);
    records = TestUtils.consumeMessage(dstBootstrapServer, topicName1, 5000);
    Assert.assertEquals(records.size(), 0);


    runnable.shutdown();
    String offset = commitZkClient.readData("/consumers/ureplicator-cluster1-cluster2/offsets/"+ topicName1 + "/0").toString();
    Assert.assertEquals(offset, "10");
  }


  @AfterTest
  public void shutdown() {
    LOGGER.info("Run after test");
    srcKafka1.shutdown();
    srcKafka2.shutdown();
    if (dstKafka != null) {
      dstKafka.shutdown();
    }
    ZkStarter.stopLocalZkServer();
  }
}
