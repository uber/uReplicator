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

import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.ManagerStarter;
import com.uber.stream.kafka.mirrormaker.manager.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.manager.utils.ZkStarter;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.restlet.Client;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.util.Random;

public class RestTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHealthCheck.class);
  private static final String MANAGER_CONTROLLER_HELIX_PREFIX = "manager-controller";
  public static String REQUEST_URL;
  public static String MANAGER_PORT = "9999";
  public static Client HTTP_CLIENT = new Client(Protocol.HTTP);
  public static ManagerStarter MANAGER_STARTER = null;
  public static String DEPLOYMENT_NAME = "testing" + new Random().nextInt(10);
  public static String HELIX_CLUSTER_NAME = MANAGER_CONTROLLER_HELIX_PREFIX + "-" + DEPLOYMENT_NAME;
  public static ZkClient ZK_CLIENT = null;
  protected KafkaServerStartable kafkaStarter;

  public static String CONTROLLER_PORT = "9998";

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
        "-maxNumWorkersPerRoute", "30",
        "-updateStatusCoolDownMs", "30"
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

  @BeforeTest
  public void setup() throws ParseException {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter = KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
        KafkaStarterUtils.DEFAULT_BROKER_ID,
        ZkStarter.DEFAULT_ZK_STR + "/cluster1", KafkaStarterUtils.getDefaultKafkaConfiguration());

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

    LOGGER.info("Trying to stop manager");
    MANAGER_STARTER.stop();

    KafkaStarterUtils.stopServer(kafkaStarter);

    ZK_CLIENT.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    ZK_CLIENT.close();
    ZkStarter.stopLocalZkServer();
  }
}
