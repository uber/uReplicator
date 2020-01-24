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

import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.common.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.ControllerStarter;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.FakeInstance;
import java.util.ArrayList;
import java.util.List;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class RestTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarterTest.class);
  public static String CONTROLLER_PORT = "9999";
  public static Client HTTP_CLIENT = new Client(Protocol.HTTP);
  public static ControllerStarter CONTROLLER_STARTER = null;
  public static String REQUEST_URL;
  public static String HELIX_CLUSTER_NAME = "ControllerStarterTest";
  public static String DEPLOYMENT_NAME = "StarterTestDeployment";
  public static List<FakeInstance> FAKE_INSTANCES = new ArrayList<FakeInstance>();
  public static ZkClient ZK_CLIENT = null;

  private static KafkaBrokerTopicObserver kafkaBrokerTopicObserver;
  private KafkaServerStartable kafkaStarter;

  @BeforeClass
  public void setup() {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    kafkaBrokerTopicObserver =
        new KafkaBrokerTopicObserver("broker0", KafkaStarterUtils.DEFAULT_ZK_STR, 1);
    kafkaBrokerTopicObserver.start();

    ZK_CLIENT = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    ZK_CLIENT.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    REQUEST_URL = "http://localhost:" + CONTROLLER_PORT;
    CONTROLLER_STARTER = startController(DEPLOYMENT_NAME, HELIX_CLUSTER_NAME, CONTROLLER_PORT);
    try {
      FAKE_INSTANCES.addAll(ControllerTestUtils
          .addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR,
              2, 0));
    } catch (Exception e) {
      throw new RuntimeException("Error during adding fake instances");
    }
  }

  @AfterClass
  public void shutdown() {
    LOGGER.info("Trying to shutdown");
    for (FakeInstance fakeInstance : FAKE_INSTANCES) {
      try {
        LOGGER.info("Trying to shutdown: " + fakeInstance);
        fakeInstance.stop();
      } catch (Exception e) {
      }
    }
    LOGGER.info("Trying to stop controller");
    CONTROLLER_STARTER.stop();
    LOGGER.info("Trying to stop zk");

    kafkaBrokerTopicObserver.shutdown();
    KafkaStarterUtils.stopServer(kafkaStarter);

    ZK_CLIENT.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    ZK_CLIENT.close();
    ZkStarter.stopLocalZkServer();
  }

  public ControllerStarter startController(String deploymentName, String helixClusterName, String port) {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerPort(port);
    conf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    conf.setHelixClusterName(helixClusterName);
    conf.setDeploymentName(deploymentName);
    conf.setBackUpToGit("false");
    conf.setAutoRebalanceDelayInSeconds("0");
    conf.setEnableAutoWhitelist("true");
    conf.setEnableAutoTopicExpansion("true");
    conf.setSrcKafkaZkPath(KafkaStarterUtils.DEFAULT_ZK_STR);
    conf.setDestKafkaZkPath(KafkaStarterUtils.DEFAULT_ZK_STR);

    final ControllerStarter starter = new ControllerStarter(conf);
    try {
      starter.start();
    } catch (Exception e) {
      throw new RuntimeException("Cannot start Helix Mirror Maker Controller", e);
    }
    return starter;
  }
}
