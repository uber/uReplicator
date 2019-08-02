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

import com.uber.stream.kafka.mirrormaker.common.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.ureplicator.common.KafkaClusterObserver;
import com.uber.stream.ureplicator.worker.interfaces.ICheckPointManager;
import com.uber.stream.ureplicator.worker.interfaces.IConsumerFetcherManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class WorkerInstanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerInstanceTest.class);

  // zk cluster 1
  private int zkCluster1Port = ZkStarter.DEFAULT_ZK_TEST_PORT;
  private String zkCluster1Str = "localhost:" + zkCluster1Port;

  // zk cluster 2
  private int zkCluster2Port = ZkStarter.DEFAULT_ZK_TEST_PORT;
  private String zkCluster2Str = "localhost:" + zkCluster2Port;

  // Source Cluster 1
  private KafkaServerStartable srcKafka1;
  private KafkaServerStartable srcKafka2;
  private final int srcCluster1Port1 = 19092;
  private final int srcCluster1Port2 = 19094;
  private final String srcCluster1ZK = zkCluster1Str + "/" + TestUtils.SRC_CLUSTER;
  private final String srcCluster1BootstrapServer = String
      .format("localhost:%d", srcCluster1Port1);

  // Source Cluster 2
  private KafkaServerStartable srcKafka3;
  private final int srcCluster2Port = 19095;
  private final String srcCluster2ZK = zkCluster2Str + "/" + TestUtils.SRC_CLUSTER_2;
  private final String srcCluster2BootstrapServer = String
      .format("localhost:%d", srcCluster2Port);

  // Destination Cluster 1
  private KafkaServerStartable dstKafka;
  private final int dstClusterPort = 19093;
  private final String dstClusterZK = zkCluster1Str + "/" + TestUtils.DST_CLUSTER;
  private final String dstBootstrapServer = String.format("localhost:%d", dstClusterPort);


  private int numberOfPartitions = 2;

  @BeforeClass
  public void setup() throws Exception {
    Properties defaultConfig = KafkaStarterUtils.getDefaultKafkaConfiguration();
    defaultConfig.setProperty("offsets.topic.replication.factor", "2");

    ZkStarter.startLocalZkServer(zkCluster1Port);

    // Starts Source Kafka Cluster1
    srcKafka1 = KafkaStarterUtils.startServer(srcCluster1Port1,
        0,
        srcCluster1ZK,
        defaultConfig);
    srcKafka1.startup();

    srcKafka2 = KafkaStarterUtils.startServer(srcCluster1Port2,
        1,
        srcCluster1ZK,
        defaultConfig);
    srcKafka2.startup();

    // Starts Source Kafka Cluster2
    srcKafka3 = KafkaStarterUtils.startServer(srcCluster2Port,
        0,
        srcCluster2ZK,
        KafkaStarterUtils.getDefaultKafkaConfiguration());
    srcKafka3.startup();

    // Starts Destination Kafka Cluster
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
      KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, srcCluster1ZK, "2");
      KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, srcCluster1ZK, "2");
      KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, dstClusterZK, "1");
      KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, dstClusterZK, "1");
      workerInstance.start(null, null, null, null);

      workerInstance.addTopicPartition(topicName1, 0);
      workerInstance.addTopicPartition(topicName1, 1);

      LOGGER.info("Add topic partition finished");
      Thread.sleep(1000);

      TestUtils.produceMessages(srcCluster1BootstrapServer, topicName1, 20, 2);
      TestUtils.produceMessages(srcCluster1BootstrapServer, topicName2, 20, 2);
      LOGGER.info("Produce messages finished");

      List<ConsumerRecord<Byte[], Byte[]>> records = TestUtils
          .consumeMessage(dstBootstrapServer, topicName1, 5000);
      Assert.assertEquals(records.size(), 20);
      LOGGER.info("Shutdown testNonFederatedWorkerInstance");
      workerInstance.cleanShutdown();
      workerInstance = null;

      workerInstance = new WorkerInstance(conf);
      workerInstance.start(null, null, null, null);

      workerInstance.addTopicPartition(topicName1, 0);
      workerInstance.addTopicPartition(topicName1, 1);
      workerInstance.addTopicPartition(topicName2, 0, 0L, null, null);
      workerInstance.addTopicPartition(topicName2, 1, 0L, 5L, null);

      Thread.sleep(1000);
      TestUtils.produceMessages(srcCluster1BootstrapServer, topicName1, 20, 2);
      TestUtils.produceMessages(srcCluster1BootstrapServer, topicName2, 20, 2);

      records = TestUtils.consumeMessage(dstBootstrapServer, topicName1, 5000);

      Assert.assertEquals(records.size(), 20);

      records = TestUtils.consumeMessage(dstBootstrapServer, topicName2, 5000);
      Assert.assertEquals(records.size(), 25);

      LOGGER.info("Shutdown worker instance");
      workerInstance.cleanShutdown();
      Assert.assertEquals(true, workerInstance.isShuttingDown.get());
    } finally {
      workerInstance.cleanShutdown();
    }
  }

  @Test
  public void testWorkerInstanceWithFetcherManagerGroupByLeaderId() throws InterruptedException {
    class CustomizedWorkerInstance extends WorkerInstance {

      public CustomizedWorkerInstance(WorkerConf workerConf) {
        super(workerConf);
      }

      @Override
      public IConsumerFetcherManager createFetcherManager() {
        KafkaClusterObserver observer = new KafkaClusterObserver(dstBootstrapServer);
        return new FetcherManagerGroupByLeaderId("FetcherManagerGroupByHashId", consumerProps,
            messageQueue, observer);
      }
    }
    WorkerConf conf = TestUtils.initWorkerConf();
    conf.setFederatedEnabled(false);
    WorkerInstance workerInstance = new CustomizedWorkerInstance(conf);
    try {
      String topicName1 = "testWorkerInstanceWithFetcherManagerGroupByLeaderId1";
      KafkaStarterUtils.createTopic(topicName1, 2, srcCluster1ZK, "2");
      KafkaStarterUtils.createTopic(topicName1, 1, dstClusterZK, "1");
      workerInstance.start(null, null, null, null);
      workerInstance.addTopicPartition(topicName1, 0);
      workerInstance.addTopicPartition(topicName1, 1);

      LOGGER.info("Add topic partition finished");
      Thread.sleep(1000);

      TestUtils.produceMessages(srcCluster1BootstrapServer, topicName1, 20, 2);
      LOGGER.info("Produce messages finished");

      List<ConsumerRecord<Byte[], Byte[]>> records = TestUtils
          .consumeMessage(dstBootstrapServer, topicName1, 5000);
      Assert.assertEquals(records.size(), 20);
      workerInstance.cleanShutdown();

      TestUtils.produceMessages(srcCluster1BootstrapServer, topicName1, 20, 2);

      workerInstance.start(null, null, null, null);
      workerInstance.addTopicPartition(topicName1, 0);
      records = TestUtils
          .consumeMessage(dstBootstrapServer, topicName1, 5000);
      Assert.assertEquals(records.size(), 10);

    } finally {
      workerInstance.cleanShutdown();
    }
  }

  @Test
  public void testFederatedWorkerEndToEnd() throws Exception {
    class WorkerStarterRunnable implements Runnable {

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

    // create new topic for testing
    String topicName1 = "tessFederatedWorkerEndToEnd1";
    String topicName2 = "tessFederatedWorkerEndToEnd2";
    String topicName3 = "tessFederatedWorkerEndToEnd3";

    KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, srcCluster1ZK, "2");
    KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, srcCluster1ZK, "2");
    KafkaStarterUtils.createTopic(topicName3, numberOfPartitions, srcCluster2ZK, "1");
    KafkaStarterUtils.createTopic(topicName1, numberOfPartitions, dstClusterZK, "1");
    KafkaStarterUtils.createTopic(topicName2, numberOfPartitions, dstClusterZK, "1");
    KafkaStarterUtils.createTopic(topicName3, numberOfPartitions, dstClusterZK, "1");

    // init zk client for zk cluster 1
    ZkClient commitZkClient = ZkUtils.createZkClient(
        srcCluster1ZK,
        10000,
        10000);
    WorkerConf workerConf = TestUtils.initWorkerConf();
    Properties helixProps = WorkerUtils.loadAndValidateHelixProps(workerConf.getHelixConfigFile());

    String instanceId = helixProps.getProperty(Constants.HELIX_INSTANCE_ID, null);
    Assert.assertNotNull(instanceId, String
        .format("failed to find property %s in configuration file %s", Constants.HELIX_INSTANCE_ID,
            workerConf.getHelixConfigFile()));

    // prepare helix cluster
    String route1 = String.format("%s-%s-0", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
    String route1ForHelix = String.format("@%s@%s", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
    String route2 = String.format("%s-%s-0", TestUtils.SRC_CLUSTER_2, TestUtils.DST_CLUSTER);
    String route2ForHelix = String.format("@%s@%s", TestUtils.SRC_CLUSTER_2, TestUtils.DST_CLUSTER);

    ZKHelixAdmin helixAdmin = TestUtils.initHelixClustersForWorkerTest(helixProps, route1, route2);
    String deployment = helixProps.getProperty("federated.deployment.name");
    String managerHelixClusterName = WorkerUtils.getManagerWorkerHelixClusterName(deployment);
    String controllerHelixClusterName = WorkerUtils.getControllerWorkerHelixClusterName(route1);
    String controller2HelixClusterName = WorkerUtils.getControllerWorkerHelixClusterName(route2);

    Thread.sleep(1000);
    WorkerStarterRunnable runnable = new WorkerStarterRunnable(workerConf);
    Thread workerThread = new Thread(runnable);
    workerThread.start();

    // assign worker to route1
    updateRouteWithValidation(managerHelixClusterName, route1ForHelix, instanceId, helixAdmin, "ONLINE");
    updateTopicWithValidation(controllerHelixClusterName, topicName1, Arrays.asList(0, 1),
        Arrays.asList("0"), helixAdmin, "ONLINE");

    TestUtils.produceMessages(srcCluster1BootstrapServer, topicName1, 20);
    List<ConsumerRecord<Byte[], Byte[]>> records = TestUtils
        .consumeMessage(dstBootstrapServer, topicName1, 5000);
    Assert.assertEquals(records.size(), 20);

    updateTopicWithValidation(controllerHelixClusterName, topicName1, Arrays.asList(0, 1),
        Arrays.asList("0"), helixAdmin, "OFFLINE");

    TestUtils.produceMessages(srcCluster1BootstrapServer, topicName1, 20);
    records = TestUtils.consumeMessage(dstBootstrapServer, topicName1, 5000);
    Assert.assertEquals(records.size(), 0);

    updateRouteWithValidation(managerHelixClusterName, route1ForHelix, instanceId, helixAdmin, "OFFLINE");


    String offset = commitZkClient
        .readData("/consumers/ureplicator-cluster1-cluster2/offsets/" + topicName1 + "/0")
        .toString();
    Assert.assertEquals(offset, "10");

    // assign worker to route1
    updateRouteWithValidation(managerHelixClusterName, route1ForHelix, instanceId, helixAdmin, "ONLINE");
    updateTopicWithValidation(controllerHelixClusterName, topicName1, Arrays.asList(0, 1),
        Arrays.asList("0"), helixAdmin, "ONLINE");
    records = TestUtils.consumeMessage(dstBootstrapServer, topicName1, 5000);
    Assert.assertEquals(records.size(), 20);


    updateRouteWithValidation(managerHelixClusterName, route1ForHelix, instanceId, helixAdmin, "OFFLINE");

    updateRouteWithValidation(managerHelixClusterName, route2ForHelix, instanceId, helixAdmin, "ONLINE");
    updateTopicWithValidation(controller2HelixClusterName, topicName3, Arrays.asList(0, 1),

        Arrays.asList("0"), helixAdmin, "ONLINE");

    TestUtils.produceMessages(srcCluster2BootstrapServer, topicName3, 20);

    records = TestUtils.consumeMessage(dstBootstrapServer, topicName3, 5000);
    Assert.assertEquals(records.size(), 20);

    runnable.shutdown();
    offset = commitZkClient
        .readData("/consumers/ureplicator-cluster1-cluster2/offsets/" + topicName1 + "/0")
        .toString();
    Assert.assertEquals(offset, "20");
  }

  private void updateRouteWithValidation(String managerHelixClusterName, String route1ForHelix,
      String instanceId, ZKHelixAdmin helixAdmin, String state)
      throws InterruptedException {
    IdealState idealState = TestUtils
        .buildManagerWorkerCustomIdealState(route1ForHelix, Collections.singletonList(instanceId),
            state);
    helixAdmin.setResourceIdealState(managerHelixClusterName, route1ForHelix, idealState);
    LOGGER.info("add resource {} for cluster {} finished", route1ForHelix, managerHelixClusterName);
    Thread.sleep(1500);
    ExternalView externalView = helixAdmin
        .getResourceExternalView(managerHelixClusterName, route1ForHelix);
    Assert.assertNotNull(externalView);
    LOGGER.info("{}", externalView);
    Assert.assertNotNull(externalView.getStateMap("0"));
    Assert.assertEquals(externalView.getStateMap("0").get("0"), state);
  }

  private void updateTopicWithValidation(String controllerHelixClusterName, String topicName,
      List<Integer> partitions, List<String> instances, ZKHelixAdmin helixAdmin, String state)
      throws InterruptedException {
    Map<String, String> partitionInstanceMap = new HashMap<>();
    for (int index = 0; index < partitions.size(); index++) {
      partitionInstanceMap
          .put(String.valueOf(partitions.get(index)), instances.get(index % instances.size()));
    }
    IdealState idealState = TestUtils
        .buildControllerWorkerCustomIdealState(topicName, partitionInstanceMap, state);
    helixAdmin.setResourceIdealState(controllerHelixClusterName, topicName, idealState);
    Thread.sleep(1000);
    ExternalView externalView = helixAdmin
        .getResourceExternalView(controllerHelixClusterName, topicName);
    for (Map.Entry<String, String> entry : partitionInstanceMap.entrySet()) {
      Assert.assertNotNull(externalView);
      Assert.assertNotNull(externalView.getStateMap(entry.getKey()));
      Assert.assertEquals(externalView.getStateMap(entry.getKey()).get(entry.getValue()), state);
    }
  }

  @AfterClass
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
