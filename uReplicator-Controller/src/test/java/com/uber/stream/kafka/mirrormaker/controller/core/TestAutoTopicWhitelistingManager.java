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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.ZkStarter;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestAutoTopicWhitelistingManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestAutoTopicWhitelistingManager.class);
  private static KafkaBrokerTopicObserver kafkaBrokerTopicObserver;
  private KafkaServerStartable kafkaStarter;
  private AutoTopicWhitelistingManager autoTopicWhitelistingManager;
  private HelixMirrorMakerManager helixMirrorMakerManager;

  @BeforeTest
  public void setup() {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }
    kafkaBrokerTopicObserver =
        new KafkaBrokerTopicObserver("broker0", KafkaStarterUtils.DEFAULT_ZK_STR, 1);

    ControllerConf controllerConf = ControllerTestUtils.initControllerConf("TestAutoTopicWhitelistingManager");

    helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();
    autoTopicWhitelistingManager = new AutoTopicWhitelistingManager(kafkaBrokerTopicObserver,
        kafkaBrokerTopicObserver, helixMirrorMakerManager, "", 1);
    autoTopicWhitelistingManager.start();
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
  public void testAutoTopic() {
    for (int i = 0; i < 10; ++i) {
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
    }
  }
}
