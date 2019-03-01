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
  public void setup() {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());
    ControllerConf controllerConf = ControllerTestUtils.initControllerConf(helixClusterName);
    controllerConf.setAutoRebalanceWorkloadRatioThreshold("0");
    controllerConf.setOffsetRefreshIntervalInSec("1");
    controllerConf.setWorkloadRefreshPeriodInSeconds("1");
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

    Thread.sleep(1000);
    Random ran = new Random();
    int partition = ran.nextInt(8);
    helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker(topicName, partition, Constants.HELIX_OFFLINE_STATE);

    IdealState idealStateForTopic =
        helixMirrorMakerManager.getIdealStateForTopic(topicName);
    ExternalView externalViewForTopic =
        helixMirrorMakerManager.getExternalViewForTopic(topicName);
    String idealState =
        idealStateForTopic.getInstanceStateMap(String.valueOf(partition)).values().iterator().next();
    String externalState =
        externalViewForTopic.getStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(idealState, "OFFLINE", "externalState idealState");
    Assert.assertEquals(externalState, "ONLINE", "unexpected externalState");


    Thread.sleep(3000);
    externalViewForTopic =
        helixMirrorMakerManager.getExternalViewForTopic(topicName);
    externalState =
        externalViewForTopic.getStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(externalState, "OFFLINE", "unexpected externalState");

    helixMirrorMakerManager.getRebalancer().triggerRebalanceCluster();

    Thread.sleep(6000);
    idealStateForTopic =
        helixMirrorMakerManager.getIdealStateForTopic(topicName);
    idealState =
        idealStateForTopic.getInstanceStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(idealState, "OFFLINE", "unexpected idealState");
    externalViewForTopic =
        helixMirrorMakerManager.getExternalViewForTopic(topicName);
    externalState =
        externalViewForTopic.getStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(externalState, "OFFLINE", "unexpected externalState");

    helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker(topicName, partition, Constants.HELIX_ONLINE_STATE);
    idealStateForTopic =
        helixMirrorMakerManager.getIdealStateForTopic(topicName);
    externalViewForTopic =
        helixMirrorMakerManager.getExternalViewForTopic(topicName);
    idealState =
        idealStateForTopic.getInstanceStateMap(String.valueOf(partition)).values().iterator().next();
    externalState =
        externalViewForTopic.getStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(idealState, "ONLINE", "unexpected idealState");
    Assert.assertEquals(externalState, "OFFLINE", "unexpected externalState");

    Thread.sleep(6000);
    externalViewForTopic =
        helixMirrorMakerManager.getExternalViewForTopic(topicName);
    externalState =
        externalViewForTopic.getStateMap(String.valueOf(partition)).values().iterator().next();
    Assert.assertEquals(externalState, "ONLINE", "unexpected externalState");
  }
}
