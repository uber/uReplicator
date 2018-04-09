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
package com.uber.stream.kafka.mirrormaker.manager.rest;

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.ControllerStarter;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.ManagerStarter;
import com.uber.stream.kafka.mirrormaker.manager.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.manager.utils.ManagerRequestURLBuilder;
import com.uber.stream.kafka.mirrormaker.manager.utils.ZkStarter;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import joptsimple.OptionSet;
import kafka.mirrormaker.ManagerWorkerHelixHandler;
import kafka.mirrormaker.ManagerWorkerOnlineOfflineStateModelFactory;
import kafka.mirrormaker.MirrorMakerWorker;
import kafka.mirrormaker.MirrorMakerWorker.FederatedMainThread;
import kafka.mirrormaker.MirrorMakerWorkerConf;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.kafka.common.utils.Utils;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Protocol;
import org.restlet.data.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestManagerTopicManagement {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestManagerTopicManagement.class);

  private static final String MANAGER_CONTROLLER_HELIX_PREFIX = "manager-controller";

  public static String REQUEST_URL;
  public static String MANAGER_PORT = "9999";
  public static String CONTROLLER_PORT = "9998";
  public static Client HTTP_CLIENT = new Client(Protocol.HTTP);
  public static ManagerStarter MANAGER_STARTER = null;
  public static String DEPLOYMENT_NAME = "testing";
  public static String HELIX_CLUSTER_NAME = MANAGER_CONTROLLER_HELIX_PREFIX + "-" + DEPLOYMENT_NAME;
  public static ZkClient ZK_CLIENT = null;
  private KafkaServerStartable kafkaStarter;
  public static List<ControllerStarter> CONTROLLER_STARTER = new ArrayList<>();
  public static List<Thread> WORKER_STARTER0 = new ArrayList<>();
  public static List<FederatedMainThread> WORKER_STARTER1 = new ArrayList<>();
  public static List<ManagerWorkerHelixHandler> WORKER_STARTER2 = new ArrayList<>();
  public static List<ZKHelixManager> WORKER_STARTER3 = new ArrayList<>();

  @BeforeTest
  public void setup() throws ParseException {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter = KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            ZkStarter.DEFAULT_ZK_STR + "/cluster1", KafkaStarterUtils.getDefaultKafkaConfiguration());

    KafkaStarterUtils.createTopic("testTopic0", ZkStarter.DEFAULT_ZK_STR + "/cluster1");

    try {
      Thread.sleep(2000);
    } catch (Exception e) {
    }

    ZK_CLIENT = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    ZK_CLIENT.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    REQUEST_URL = "http://localhost:" + MANAGER_PORT;
    MANAGER_STARTER = startManager(DEPLOYMENT_NAME, MANAGER_PORT);

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @AfterTest
  public void shutdown() {
    LOGGER.info("Trying to shutdown");

    LOGGER.info("Trying to stop worker");
    for (int i=0; i<WORKER_STARTER1.size(); i++) {
      WORKER_STARTER1.get(i).shutdown();
      WORKER_STARTER2.get(i).stop();
      WORKER_STARTER3.get(i).disconnect();
    }

    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    LOGGER.info("Trying to stop controller");
    for (ControllerStarter controllerStarter : CONTROLLER_STARTER) {
      controllerStarter.stop();
    }

    LOGGER.info("Trying to stop manager");
    MANAGER_STARTER.stop();

    KafkaStarterUtils.stopServer(kafkaStarter);

    ZK_CLIENT.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    ZK_CLIENT.close();
    ZkStarter.stopLocalZkServer();
  }

  public ManagerStarter startManager(String deplymentName, String port)
      throws ParseException {
    String[] args = new String[]{
        "-env", "testing1",
        "-srcClusters", "cluster1",
        "-destClusters", "cluster3",
        "-zookeeper", ZkStarter.DEFAULT_ZK_STR,
        "-managerPort", port,
        "-deployment", deplymentName,
        "-instanceId", "instance0",
        "-controllerPort", CONTROLLER_PORT,
        "-c3Host", "testhost",
        "-c3Port", "8081",
        "-workloadRefreshPeriodInSeconds", "10",
        "-initMaxNumPartitionsPerRoute", "20",
        "-maxNumPartitionsPerRoute", "30",
        "-initMaxNumWorkersPerRoute", "20",
        "-maxNumWorkersPerRoute", "30"
    };
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
    ManagerConf conf = ManagerConf.getManagerConf(cmd);
    conf.addProperty("kafka.cluster.zkStr.cluster1", ZkStarter.DEFAULT_ZK_STR + "/cluster1");

    ManagerStarter managerStarter = new ManagerStarter(conf);
    try {
      managerStarter.start();
    } catch (Exception e) {
      throw new RuntimeException("Cannot start Helix Mirror Maker Controller");
    }
    return managerStarter;
  }

  public void startController(String deploymentName, String port, int numController) {
    for (int i = 0; i < numController; i++) {
      int id = i;
      new Thread(new Runnable() {
        @Override
        public void run() {
          final ControllerConf conf = new ControllerConf();
          conf.setControllerPort(port);
          conf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
          conf.setInstanceId("localhost");
          conf.setFederatedEnabled("true");
          conf.setDeploymentName(deploymentName);
          conf.setBackUpToGit("false");
          conf.setAutoRebalanceDelayInSeconds("0");
          conf.setEnableAutoWhitelist("true");
          conf.setEnableAutoTopicExpansion("true");
          conf.setSourceClusters("cluster1");
          conf.setDestinationClusters("cluster3");
          conf.addProperty("kafka.cluster.zkStr.cluster1", ZkStarter.DEFAULT_ZK_STR + "/cluster1");
          conf.addProperty("kafka.cluster.zkStr.cluster3", ZkStarter.DEFAULT_ZK_STR + "/cluster3");

          final ControllerStarter controllerStarter = new ControllerStarter(conf);
          try {
            CONTROLLER_STARTER.add(id, controllerStarter);
            controllerStarter.start();
          } catch (Exception e) {
            throw new RuntimeException("Cannot start Helix Mirror Maker Controller");
          }
        }
      }).start();
    }
  }

  public void startWorker(String deploymentName, int numWorker) {
    LOGGER.info("start worker");

    for (int i = 0; i < numWorker; i++) {
      int id = i;
      new Thread(new Runnable() {
        @Override
        public void run() {
          // start the worker in federated mode
          String zkServer = ZkStarter.DEFAULT_ZK_STR;
          String instanceId = "testWorker" + id;
          String managerWorkerHelixName = "manager-worker-" + deploymentName;

          MirrorMakerWorkerConf workerConf = new MirrorMakerWorkerConf();
          String[] args = new String[]{
              "--consumer.config", "/tmp/consumer.properties-" + id,
              "--producer.config", "/tmp/producer.properties-" + id,
              "--helix.config", "/tmp/helix.properties-" + id,
              "--cluster.config", "src/test/resources/clusters.properties"
          };

          MirrorMakerWorker mirrorMakerWorker = new MirrorMakerWorker();

          updateConsumerConfigFile(zkServer, instanceId, id);
          updateProducerConfigFile(instanceId, id);
          updateHelixConfigFile(zkServer, instanceId, id);

          OptionSet options = workerConf.getParser().parse(args);

          ZKHelixManager managerWorkerHelix = new ZKHelixManager(managerWorkerHelixName, instanceId,
              InstanceType.PARTICIPANT, zkServer);
          StateMachineEngine stateMachineEngine = managerWorkerHelix.getStateMachineEngine();
          ManagerWorkerHelixHandler managerWorkerHandler = new ManagerWorkerHelixHandler(mirrorMakerWorker, workerConf, options);
          // register the MirrorMaker worker to Manager-Worker cluster
          ManagerWorkerOnlineOfflineStateModelFactory stateModelFactory = new ManagerWorkerOnlineOfflineStateModelFactory(
              managerWorkerHandler);
          stateMachineEngine.registerStateModelFactory("OnlineOffline", stateModelFactory);

          FederatedMainThread mainThread = new FederatedMainThread();
          Runtime.getRuntime().addShutdownHook(new Thread("MirrorMakerShutdownHook") {
            public void run() {
              LOGGER.info("Shutting down federated worker");
              mainThread.shutdown();
              managerWorkerHandler.stop();
              managerWorkerHelix.disconnect();
            }
          });

          try {
            managerWorkerHelix.connect();
          } catch (Exception e) {
            e.printStackTrace();
          }
          mainThread.start();
          WORKER_STARTER1.add(mainThread);
          WORKER_STARTER2.add(managerWorkerHandler);
          WORKER_STARTER3.add(managerWorkerHelix);
        }
      }).start();
    }
  }

  void updateConsumerConfigFile(String zk, String instanceId, int id) {
    // load consumer config from file
    String consumerFileName = "src/test/resources/consumer.properties";
    PropertiesConfiguration consumerConfigFromFile = new PropertiesConfiguration();
    consumerConfigFromFile.setDelimiterParsingDisabled(true);
    try {
      consumerConfigFromFile.load(consumerFileName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config from file " + consumerFileName + ": " + e.getMessage());
    }

    OutputStream output = null;

    try {
      output = new FileOutputStream("/tmp/consumer.properties-" + id);
      // set the properties value
      consumerConfigFromFile.setProperty("zookeeper.connect", zk);
      consumerConfigFromFile.setProperty("client.id", instanceId);
      // save properties to project tmp folder
      consumerConfigFromFile.save(output);

    } catch (Exception io) {
      io.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  void updateProducerConfigFile(String instanceId, int id) {
    // load producer config from file
    String producerFileName = "src/test/resources/producer.properties";
    PropertiesConfiguration producerConfigFromFile = new PropertiesConfiguration();
    producerConfigFromFile.setDelimiterParsingDisabled(true);
    try {
      producerConfigFromFile.load(producerFileName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config from file " + producerFileName + ": " + e.getMessage());
    }

    OutputStream output = null;

    try {
      output = new FileOutputStream("/tmp/producer.properties-" + id);
      // set the properties value
      producerConfigFromFile.setProperty("bootstrap.servers", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
      producerConfigFromFile.setProperty("client.id", instanceId);
      // save properties to project tmp folder
      producerConfigFromFile.save(output);

    } catch (Exception io) {
      io.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  void updateHelixConfigFile(String zk, String instanceId, int id) {
    // load helix config from file
    String helixFileName = "src/test/resources/helix.properties";
    PropertiesConfiguration helixConfigFromFile = new PropertiesConfiguration();
    helixConfigFromFile.setDelimiterParsingDisabled(true);
    try {
      helixConfigFromFile.load(helixFileName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load config from file " + helixFileName + ": " + e.getMessage());
    }

    OutputStream output = null;

    try {
      output = new FileOutputStream("/tmp/helix.properties-" + id);
      // set the properties value
      helixConfigFromFile.setProperty("federated.deployment.name", DEPLOYMENT_NAME);
      helixConfigFromFile.setProperty("zkServer", zk);
      helixConfigFromFile.setProperty("instanceId", instanceId);
      // save properties to project tmp folder
      helixConfigFromFile.save(output);

    } catch (Exception io) {
      io.printStackTrace();
    } finally {
      if (output != null) {
        try {
          output.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  class FederatedMainThread extends Thread {
    private CountDownLatch latch = new CountDownLatch(1);

    void shutdown() {
      latch.countDown();
    }

    public void run() {
      LOGGER.info("Starting federated main thread");
      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testGet() {
    // Get call
    Request request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("");
    Response response = HTTP_CLIENT.handle(request);
    JSONObject json = JSONObject.parseObject(response.getEntityAsText());
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(json.getString("message"), "No topic is added in uReplicator!");
    Assert.assertEquals(json.getString("status"), "404");

    // Create topic but expect failure with no controller
    request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicCreationRequestUrl("testTopic0", "cluster1", "cluster3");

    response = HTTP_CLIENT.handle(request);
    System.out.println("request:" + request);
    System.out.println("response:" + response.getEntityAsText());
    Assert.assertEquals(response.getStatus(), Status.SERVER_ERROR_INTERNAL);
    Assert.assertFalse(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic0"));

    // Add controller
    startController(DEPLOYMENT_NAME, CONTROLLER_PORT, 1);
    startWorker(DEPLOYMENT_NAME, 2);
    try {
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    response = HTTP_CLIENT.handle(request);
    System.out.println("request:" + request);
    System.out.println("response:" + response.getEntityAsText());
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertTrue(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic0"));

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("");
    response = HTTP_CLIENT.handle(request);
    json = JSONObject.parseObject(response.getEntityAsText());
    System.out.println("request:" + request);
    System.out.println("response:" + response.getEntityAsText());
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(json.getJSONObject("message").getString("topics"), "[\"testTopic0\"]");
    Assert.assertEquals(json.getString("status"), "200");

    // Get call
    request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("testTopic0");
    response = HTTP_CLIENT.handle(request);
    System.out.println("request:" + request);
    System.out.println("response:" + response.getEntityAsText());
    json = JSONObject.parseObject(response.getEntityAsText());
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(json.getJSONObject("message").getJSONObject("managerView").getString("topic"), "testTopic0");
    Assert.assertEquals(json.getString("status"), "200");

    // Delete topic
    request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicDeleteRequestUrl("testTopic0", "cluster1", "cluster3");
    response = HTTP_CLIENT.handle(request);

    json = JSONObject.parseObject(response.getEntityAsText());
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(json.getString("message"), "Successfully delete topic: testTopic0 from cluster1 to cluster3");
    Assert.assertEquals(json.getString("status"), "200");
  }

}
