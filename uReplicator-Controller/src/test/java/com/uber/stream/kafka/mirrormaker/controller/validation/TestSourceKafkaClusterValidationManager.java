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
package com.uber.stream.kafka.mirrormaker.controller.validation;

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.common.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestSourceKafkaClusterValidationManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestSourceKafkaClusterValidationManager.class);
  private static KafkaBrokerTopicObserver kafkaBrokerTopicObserver;
  private KafkaServerStartable kafkaStarter;
  private HelixMirrorMakerManager helixMirrorMakerManager;
  private SourceKafkaClusterValidationManager sourceKafkaClusterValidationManager;

  @BeforeTest
  public void setup() {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    try {
      Thread.sleep(2000);
    } catch (Exception e) {
    }
    kafkaBrokerTopicObserver =
        new KafkaBrokerTopicObserver("broker0", KafkaStarterUtils.DEFAULT_ZK_STR, 1);
    kafkaBrokerTopicObserver.start();

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerPort("9090");
    controllerConf.setHelixClusterName("TestAutoTopicWhitelistingManager");
    controllerConf.setDeploymentName("TestAutoTopicWhitelistingManagerDeployment");
    controllerConf.setInstanceId("controller-0");
    controllerConf.setControllerMode("customized");
    controllerConf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    controllerConf.setBackUpToGit("false");
    controllerConf.setAutoRebalanceDelayInSeconds("1");

    helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();
    sourceKafkaClusterValidationManager =
        new SourceKafkaClusterValidationManager(kafkaBrokerTopicObserver, helixMirrorMakerManager);
    sourceKafkaClusterValidationManager.start();
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
    String validationResult = sourceKafkaClusterValidationManager.validateSourceKafkaCluster();
    System.out.println(validationResult);
    Assert.assertEquals(validationResult,
        "{\"numMissingTopics\":0,\"numMismatchedTopics\":0,\"mismatchedTopicPartitions\":{},\"numMismatchedTopicPartitions\":0}");
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(helixMirrorMakerManager.getTopicLists().size(), i);
      String topicName = "testTopic" + i;
      // Create Kafka topic
      KafkaStarterUtils.createTopic(topicName, KafkaStarterUtils.DEFAULT_ZK_STR);
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      Assert.assertEquals(kafkaBrokerTopicObserver.getNumTopics(), 1 + i);
      for (int j = 0; j <= i; ++j) {
        Assert.assertTrue(kafkaBrokerTopicObserver.getAllTopics().contains("testTopic" + j));
        Assert.assertEquals(
            kafkaBrokerTopicObserver.getTopicPartition("testTopic" + j).getPartition(), 1);
      }

      validationResult = sourceKafkaClusterValidationManager.validateSourceKafkaCluster();
      System.out.println(validationResult);
      Assert.assertEquals(validationResult,
        "{\"numMissingTopics\":0,\"numMismatchedTopics\":0,\"mismatchedTopicPartitions\":{},\"numMismatchedTopicPartitions\":0}");
      helixMirrorMakerManager.addTopicToMirrorMaker(topicName, 1);
    }

    for (int i = 2; i < 4; ++i) {
      helixMirrorMakerManager.addTopicToMirrorMaker("testTopic" + i, 1);
    }
    JSONObject validationResultJson =
        JSONObject.parseObject(sourceKafkaClusterValidationManager.validateSourceKafkaCluster());
    System.out.println(validationResultJson);
    Assert.assertEquals(validationResultJson.get("numMissingTopics"), 2);
    for (int i = 2; i < 4; ++i) {
      Assert.assertEquals(helixMirrorMakerManager.getTopicLists().size(), 4);
      String topicName = "testTopic" + i;
      // Create Kafka topic
      KafkaStarterUtils.createTopic(topicName, KafkaStarterUtils.DEFAULT_ZK_STR);
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      Assert.assertEquals(kafkaBrokerTopicObserver.getNumTopics(), 1 + i);
      for (int j = 0; j <= i; ++j) {
        Assert.assertTrue(kafkaBrokerTopicObserver.getAllTopics().contains("testTopic" + j));
        Assert.assertEquals(
            kafkaBrokerTopicObserver.getTopicPartition("testTopic" + j).getPartition(), 1);
      }

      validationResultJson =
          JSONObject.parseObject(sourceKafkaClusterValidationManager.validateSourceKafkaCluster());
      System.out.println(validationResultJson);
      Assert.assertEquals(validationResultJson.get("numMissingTopics"), 3 - i);
    }
    for (int i = 4; i < 6; ++i) {
      helixMirrorMakerManager.addTopicToMirrorMaker("testTopic" + i, 2);
    }
    validationResultJson =
        JSONObject.parseObject(sourceKafkaClusterValidationManager.validateSourceKafkaCluster());
    System.out.println(validationResultJson);
    Assert.assertEquals(validationResultJson.get("numMissingTopics"), 2);
    for (int i = 4; i < 6; ++i) {
      Assert.assertEquals(helixMirrorMakerManager.getTopicLists().size(), 6);
      String topicName = "testTopic" + i;
      // Create Kafka topic
      KafkaStarterUtils.createTopic(topicName, KafkaStarterUtils.DEFAULT_ZK_STR);
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      Assert.assertEquals(kafkaBrokerTopicObserver.getNumTopics(), 1 + i);
      for (int j = 0; j <= i; ++j) {
        Assert.assertTrue(kafkaBrokerTopicObserver.getAllTopics().contains("testTopic" + j));
        Assert.assertEquals(
            kafkaBrokerTopicObserver.getTopicPartition("testTopic" + j).getPartition(), 1);
      }

      validationResultJson =
          JSONObject.parseObject(sourceKafkaClusterValidationManager.validateSourceKafkaCluster());
      System.out.println(validationResultJson);
      Assert.assertEquals(validationResultJson.get("numMissingTopics"), 5 - i);
      Assert.assertEquals(validationResultJson.get("numMismatchedTopics"), i - 3);
      Assert.assertEquals(validationResultJson.get("numMismatchedTopicPartitions"), i - 3);
      for (int h = 4; h <= i; ++h) {
        Assert.assertEquals(
            validationResultJson.getJSONObject("mismatchedTopicPartitions").get("testTopic" + h),
            1);
      }
    }

    for (int i = 4; i < 6; ++i) {
      helixMirrorMakerManager.deleteTopicInMirrorMaker("testTopic" + i);
      helixMirrorMakerManager.addTopicToMirrorMaker("testTopic" + i, 1);
    }
    validationResult = sourceKafkaClusterValidationManager.validateSourceKafkaCluster();
    System.out.println(validationResult);

    Assert.assertEquals(validationResult,
        "{\"numMissingTopics\":0,\"numMismatchedTopics\":0,\"mismatchedTopicPartitions\":{},\"numMismatchedTopicPartitions\":0}");
   
  }

}
