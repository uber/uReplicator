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

import com.uber.stream.kafka.mirrormaker.common.Constants;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.ZkStarter;
import kafka.server.KafkaServerStartable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Random;

public class TestTopicPartitionBlacklist {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestTopicPartitionBlacklist.class);
  private KafkaServerStartable kafkaStarter;
  private HelixMirrorMakerManager helixMirrorMakerManager;
  private final String helixClusterName = "TestAutoTopicWhitelistingManager";

  @BeforeTest
  public void setup() throws InterruptedException {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());
    ControllerConf controllerConf = ControllerTestUtils.initControllerConf(helixClusterName);
    controllerConf.setAutoRebalanceWorkloadRatioThreshold("0");
    controllerConf.setWorkloadRefreshPeriodInSeconds("5");
    controllerConf.setSrcKafkaZkPath("localhost:2181/uReplicator/testDeployment");

    helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();
  }

  @AfterTest
  public void shutdown() {
    LOGGER.info("Trying to shutdown");
    helixMirrorMakerManager.stop();
    KafkaStarterUtils.stopServer(kafkaStarter);
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testBlacklistTopicPartition() throws Exception {
    int numBatchBringUpInstances = 2;
    LOGGER.info("Trying to add {} instances", numBatchBringUpInstances);
    ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR,
            numBatchBringUpInstances, 0);
    String topicName = "testTopic";
    helixMirrorMakerManager.addTopicToMirrorMaker(topicName, 8);

    Random ran = new Random();
    int partition = ran.nextInt(8);
    assertIdealStateOnce(topicName, partition, "ONLINE");
    assertExternalView(topicName, partition, "ONLINE");

    helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker(topicName, partition, Constants.HELIX_OFFLINE_STATE);

    assertIdealStateOnce(topicName, partition, "OFFLINE");
    assertExternalViewOnce(topicName, partition, "ONLINE");
    assertExternalView(topicName, partition, "OFFLINE");

    helixMirrorMakerManager.getRebalancer().triggerRebalanceCluster();

    assertIdealStateOnce(topicName, partition, "OFFLINE");
    assertExternalView(topicName, partition, "OFFLINE");

    helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker(topicName, partition, Constants.HELIX_ONLINE_STATE);
    assertIdealStateOnce(topicName, partition, "ONLINE");
    assertExternalViewOnce(topicName, partition, "OFFLINE");
    assertExternalView(topicName, partition, "ONLINE");
  }

  private void assertExternalView(String topicName, int partition, String expectedState) throws InterruptedException {
    int maxRetry = 10;
    for (int i = 0; i < maxRetry; i++) {

      try {
        if (assertExternalViewOnce(topicName, partition, expectedState)) {
          return;
        }
      } catch (AssertionError e) {
        LOGGER.info("assertExternalViewOnce failed, try in next 1000ms, retry count: {}", i);
        Thread.sleep(1000);
      }
    }
    assertExternalViewOnce(topicName, partition, expectedState);
  }

  private boolean assertExternalViewOnce(String topicName, int partition, String expected) {
    ExternalView externalViewForTopic =
        helixMirrorMakerManager.getExternalViewForTopic(topicName);
    if (externalViewForTopic == null ||
        externalViewForTopic.getStateMap(String.valueOf(partition)) == null ||
        externalViewForTopic.getStateMap(String.valueOf(partition)).values().size() == 0) {
      Assert.fail(String.format("fail to find ExternalView for topic %s, partition %d", topicName, partition));
    }
    String externalState =
        externalViewForTopic.getStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(externalState, expected, "unexpected externalState");
    return true;
  }

  private boolean assertIdealStateOnce(String topicName, int partition, String expected) {
    IdealState idealStateForTopic =
        helixMirrorMakerManager.getIdealStateForTopic(topicName);
    if (idealStateForTopic.getInstanceStateMap(String.valueOf(partition)) == null || idealStateForTopic.getInstanceStateMap(String.valueOf(partition)).values().size() == 0) {
      Assert.fail(String.format("fail to find IdealState for topic %s, partition %d", topicName, partition));
    }
    String externalState =
        idealStateForTopic.getInstanceStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(externalState, expected, "unexpected idealstate");
    return true;
  }

}
