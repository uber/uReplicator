/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.validation;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.core.AutoTopicWhitelistingManager;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.FakeInstance;
import com.uber.stream.kafka.mirrormaker.controller.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.ZkStarter;

import kafka.server.KafkaServerStartable;

public class TestValidationManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestValidationManager.class);
  private static KafkaBrokerTopicObserver kafkaBrokerTopicObserver;
  private KafkaServerStartable kafkaStarter;
  private HelixMirrorMakerManager helixMirrorMakerManager;
  private ValidationManager validationManager;
  private final String helixClusterName = "TestAutoTopicWhitelistingManager";
  public static List<FakeInstance> fakeInstances = new ArrayList<FakeInstance>();

  @BeforeTest
  public void setup() {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    // Create Kafka topic
    KafkaStarterUtils.createTopic("testTopic0", KafkaStarterUtils.DEFAULT_ZK_STR);
    try {
      Thread.sleep(2000);
    } catch (Exception e) {
    }
    kafkaBrokerTopicObserver =
        new KafkaBrokerTopicObserver("broker0", KafkaStarterUtils.DEFAULT_ZK_STR);

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerPort("9090");
    controllerConf.setHelixClusterName(helixClusterName);
    controllerConf.setInstanceId("controller-0");
    controllerConf.setControllerMode("customized");
    controllerConf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    controllerConf.setBackUpToGit("false");
    controllerConf.setAutoRebalanceDelayInSeconds("1");

    helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();

    try {
      fakeInstances.addAll(ControllerTestUtils
          .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR,
              4, 0));
      Thread.sleep(4000);
    } catch (Exception e) {
      throw new RuntimeException("Error during adding fake instances");
    }

    validationManager = new ValidationManager(helixMirrorMakerManager);
    validationManager.start();
  }

  @AfterTest
  public void shutdown() {
    LOGGER.info("Trying to shutdown");
    helixMirrorMakerManager.stop();
    kafkaBrokerTopicObserver.stop();
    KafkaStarterUtils.stopServer(kafkaStarter);
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testValidation() {
    helixMirrorMakerManager.addTopicToMirrorMaker("testTopic0", 1);
    // {"ExternalView":{"Server_localhost_0":1},
    // "IdealState":{"Server_localhost_0":1},
    // "numErrorTopicPartitions":0,
    // "numErrorTopics":0,
    // "numOfflineTopicPartitions":0,
    // "numOnlineTopicPartitions":1,
    // "numTopicPartitions":1,
    // "numTopics":1}
    try {
      Thread.sleep(3000);
    } catch (Exception e) {
    }
    JSONObject validationResultJson =
        JSONObject.parseObject(validationManager.validateExternalView());
    Assert.assertEquals(validationResultJson.getIntValue("numErrorTopicPartitions"), 0);
    Assert.assertEquals(validationResultJson.getIntValue("numErrorTopics"), 0);
    Assert.assertEquals(validationResultJson.getIntValue("numOfflineTopicPartitions"), 0);
    Assert.assertEquals(validationResultJson.getIntValue("numOnlineTopicPartitions"), 1);
    Assert.assertEquals(validationResultJson.getIntValue("numTopicPartitions"), 1);
    Assert.assertEquals(validationResultJson.getIntValue("numTopics"), 1);
    helixMirrorMakerManager.addTopicToMirrorMaker("testTopic1", 2);
    try {
      Thread.sleep(3000);
    } catch (Exception e) {
    }
    validationResultJson = JSONObject.parseObject(validationManager.validateExternalView());
    Assert.assertEquals(validationResultJson.getIntValue("numErrorTopicPartitions"), 0);
    Assert.assertEquals(validationResultJson.getIntValue("numErrorTopics"), 0);
    Assert.assertEquals(validationResultJson.getIntValue("numOfflineTopicPartitions"), 0);
    Assert.assertEquals(validationResultJson.getIntValue("numOnlineTopicPartitions"), 3);
    Assert.assertEquals(validationResultJson.getIntValue("numTopicPartitions"), 3);
    Assert.assertEquals(validationResultJson.getIntValue("numTopics"), 2);
  }

}
