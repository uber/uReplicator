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
package com.uber.stream.kafka.mirrormaker.controller.integration;

import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.FakeInstance;
import com.uber.stream.kafka.mirrormaker.controller.utils.ZkStarter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class HelixMirrorMakerManagerCustomEmptyFullTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HelixMirrorMakerManagerCustomEmptyFullTest.class);

  @BeforeTest
  public void setup() {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
  }

  @AfterTest
  public void shutdown() {
    LOGGER.info("Trying to shutdown");
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testControllerStarter() throws Exception {
    LOGGER.info("testControllerStarter");
    String helixClusterName = "HelixMirrorMakerManagerCustomEmptyFullTest";
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerPort("9090");
    controllerConf.setHelixClusterName(helixClusterName);
    controllerConf.setInstanceId("controller-0");
    controllerConf.setControllerMode("customized");
    controllerConf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    controllerConf.setBackUpToGit("false");
    controllerConf.setAutoRebalanceDelayInSeconds("1");
    HelixMirrorMakerManager helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();
    // Adding fake workers
    int numBatchBringUpInstances = 10;
    int numTotalTopics = 10;
    for (int i = 0; i < numTotalTopics; ++i) {
      String topic = "testTopic" + i;
      LOGGER.info("Trying to add topic {}", topic);
      helixMirrorMakerManager.addTopicToMirrorMaker(topic, 8);
      Thread.sleep(500);
    }
    assertEmptyCluster(helixMirrorMakerManager);

    LOGGER.info("Trying to add {} instances", numBatchBringUpInstances);
    List<FakeInstance> fakeInstances = ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR,
            numBatchBringUpInstances, 0);
    Thread.sleep(5000);
    assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager,
        numBatchBringUpInstances, 8 * numTotalTopics);

    LOGGER.info("Trying to add {} more instances, waiting for rebalancing",
        numBatchBringUpInstances);
    fakeInstances.addAll(ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR,
            numBatchBringUpInstances, numBatchBringUpInstances));
    Thread.sleep(5000);
    assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager,
        numBatchBringUpInstances * 2, 8 * numTotalTopics);

    for (int i = 0; i < numTotalTopics; ++i) {
      String topic = "testTopic" + i;
      LOGGER.info("Expanding topic: {} , waiting for rebalancing", topic);
      helixMirrorMakerManager.expandTopicInMirrorMaker(topic, 16);
      Thread.sleep(5000);
      assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager,
          numBatchBringUpInstances * 2, 8 * numTotalTopics + 8 * (i + 1));
    }

    LOGGER.info("Simulate restart nodes 1 by 1");
    int totalInstancesSize = fakeInstances.size();
    for (int i = 0; i < totalInstancesSize * 2; ++i) {
      if (i % 2 == 0) {
        LOGGER.info("Trying to bring down: " + fakeInstances.get(i / 2).getInstanceId());
        fakeInstances.get(i / 2).stop();
        totalInstancesSize--;
      } else {
        LOGGER.info("Trying to bring up: " + fakeInstances.get(i / 2).getInstanceId());
        fakeInstances.get(i / 2).start();
        totalInstancesSize++;
      }
      Thread.sleep(5000);
      assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager, totalInstancesSize,
          16 * numTotalTopics);
    }

    LOGGER.info("Bring down nodes 1 by 1");
    for (int i = 0; i < fakeInstances.size() - 1; ++i) {
      LOGGER.info("Trying to bring down: " + fakeInstances.get(i).getInstanceId());
      fakeInstances.get(i).stop();
      totalInstancesSize--;
      Thread.sleep(5000);
      assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager, totalInstancesSize,
          16 * numTotalTopics);
    }

    LOGGER.info("Bring up nodes 1 by 1");
    for (int i = 0; i < fakeInstances.size() - 1; ++i) {
      LOGGER.info("Trying to bring up: " + fakeInstances.get(i).getInstanceId());
      fakeInstances.get(i).start();
      totalInstancesSize++;
      Thread.sleep(5000);
      assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager, totalInstancesSize,
          16 * numTotalTopics);
    }

    helixMirrorMakerManager.stop();
  }

  private void assertEmptyCluster(HelixMirrorMakerManager helixMirrorMakerManager) {
    for (String topicName : helixMirrorMakerManager.getTopicLists()) {
      Assert.assertNull(
          helixMirrorMakerManager.getExternalViewForTopic(topicName));
      Assert.assertEquals(
          helixMirrorMakerManager.getIdealStateForTopic(topicName).getPartitionSet().size(), 0);
    }
  }

  private void assertInstanceOwnedTopicPartitionsBalanced(
      HelixMirrorMakerManager helixMirrorMakerManager,
      int numInstances, int numTotalPartitions) {
    Map<String, Integer> serverToPartitionMapping = new HashMap<String, Integer>();
    for (String topicName : helixMirrorMakerManager.getTopicLists()) {
      ExternalView externalViewForTopic =
          helixMirrorMakerManager.getExternalViewForTopic(topicName);
      LOGGER.info("ExternalView: " + externalViewForTopic.toString());
      for (String partition : externalViewForTopic.getPartitionSet()) {
        String instanceName =
            externalViewForTopic.getStateMap(partition).keySet().iterator().next();
        if (!serverToPartitionMapping.containsKey(instanceName)) {
          serverToPartitionMapping.put(instanceName, 0);
        }
        serverToPartitionMapping.put(instanceName, serverToPartitionMapping.get(instanceName) + 1);
      }
    }
    int expectedLowerBound = (int) Math.floor((double) numTotalPartitions / (double) numInstances);
    int expectedUpperBound = (int) Math.ceil((double) numTotalPartitions / (double) numInstances);
    for (String instanceName : serverToPartitionMapping.keySet()) {
      // May not be perfect balancing.
      LOGGER.info("Current {} serving {} partitions, expected [{}, {}]", instanceName,
          serverToPartitionMapping.get(instanceName), expectedLowerBound,
          expectedUpperBound);
      Assert.assertTrue(serverToPartitionMapping.get(instanceName) >= expectedLowerBound);
      Assert.assertTrue(serverToPartitionMapping.get(instanceName) <= expectedUpperBound);
    }
  }

}
