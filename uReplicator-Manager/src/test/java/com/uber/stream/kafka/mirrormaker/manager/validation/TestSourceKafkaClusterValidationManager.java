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
package com.uber.stream.kafka.mirrormaker.manager.validation;

import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.controller.ControllerStarter;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.rest.TestManagerTopicManagement;
import com.uber.stream.kafka.mirrormaker.manager.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.manager.utils.ZkStarter;
import java.util.HashMap;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

/**
 * Validate idealstates and source kafka cluster info and update related metrics.
 */
public class TestSourceKafkaClusterValidationManager {

  /*private static final Logger LOGGER = LoggerFactory.getLogger(TestSourceKafkaClusterValidationManager.class);

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
  }*/

}
