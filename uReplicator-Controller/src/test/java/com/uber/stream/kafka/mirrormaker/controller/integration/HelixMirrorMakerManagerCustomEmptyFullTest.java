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

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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
    ControllerConf controllerConf = ControllerTestUtils.initControllerConf(helixClusterName);
    controllerConf.setAutoRebalanceWorkloadRatioThreshold("0");
    HelixMirrorMakerManager helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();
    // Adding fake workers
    Random random = new Random();
    int numBatchBringUpInstances = 3 + random.nextInt(3);
    int numTotalTopics = 3 + random.nextInt(3);
    for (int i = 0; i < numTotalTopics; ++i) {
      String topic = "testTopic" + i;
      helixMirrorMakerManager.addTopicToMirrorMaker(topic, 8);
    }
    assertEmptyCluster(helixMirrorMakerManager);
    Assert.assertEquals(helixMirrorMakerManager.getTopicLists().size(), numTotalTopics, "number of topic not match");

    LOGGER.info("Trying to add {} instances", numBatchBringUpInstances);
    List<FakeInstance> fakeInstances = ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR,
            numBatchBringUpInstances, 0);
    Thread.sleep(6000);
    ControllerTestUtils.assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager,
        numBatchBringUpInstances, 8 * numTotalTopics);

    LOGGER.info("Trying to add {} more instances, waiting for rebalancing",
        numBatchBringUpInstances);
    fakeInstances.addAll(ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR,
            numBatchBringUpInstances, numBatchBringUpInstances));

    Thread.sleep(6000);
    ControllerTestUtils.assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager,
        numBatchBringUpInstances * 2, 8 * numTotalTopics);

    for (int i = 0; i < numTotalTopics; ++i) {
      String topic = "testTopic" + i;
      LOGGER.info("Expanding topic: {} , waiting for rebalancing", topic);
      helixMirrorMakerManager.expandTopicInMirrorMaker(topic, 16);
    }
    Thread.sleep(6000);
    ControllerTestUtils.assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager,
        numBatchBringUpInstances * 2, 16 * numTotalTopics);

    LOGGER.info("Simulate restart nodes 1 by 1");
    int totalInstancesSize = numBatchBringUpInstances * 2;
    int eventCount = 1 + random.nextInt(fakeInstances.size() - 1);
    for (int i = 0; i < eventCount; ++i) {
      int instanceId = random.nextInt(fakeInstances.size());
      LOGGER.info("Trying to bring down: " + fakeInstances.get(instanceId).getInstanceId());
      totalInstancesSize = stopInstance(fakeInstances.get(instanceId), totalInstancesSize);
      LOGGER.info("Trying to bring up: " + fakeInstances.get(instanceId).getInstanceId());
      totalInstancesSize = startInstance(fakeInstances.get(instanceId), totalInstancesSize);
      Thread.sleep(6000);
      ControllerTestUtils.assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager, totalInstancesSize,
          16 * numTotalTopics);
    }

    LOGGER.info("Bring down nodes 1 by 1");
    Set<Integer> stoppedId = new HashSet<>();
    for (int i = 0; i < 3; ++i) {
      int instanceId = random.nextInt(fakeInstances.size());
      if (stoppedId.contains(instanceId)) {
        continue;
      }
      stoppedId.add(instanceId);
      LOGGER.info("Trying to bring down: " + fakeInstances.get(instanceId).getInstanceId());
      totalInstancesSize = stopInstance(fakeInstances.get(instanceId), totalInstancesSize);

    }
    Thread.sleep(6000);
    ControllerTestUtils.assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager, totalInstancesSize,
        16 * numTotalTopics);

    LOGGER.info("Bring up nodes 1 by 1");
    for (int i = 0; i < stoppedId.size(); ++i) {
      int instanceId = (Integer) stoppedId.toArray()[i];
      LOGGER.info("Trying to bring up: " + fakeInstances.get(instanceId).getInstanceId());
      totalInstancesSize = startInstance(fakeInstances.get(instanceId), totalInstancesSize);
      Thread.sleep(6000);
      ControllerTestUtils.assertInstanceOwnedTopicPartitionsBalanced(helixMirrorMakerManager, totalInstancesSize,
          16 * numTotalTopics);
    }

    helixMirrorMakerManager.stop();
  }

  private int stopInstance(FakeInstance fakeInstance, int totalInstancesSize) throws Exception {
    fakeInstance.stop();
    totalInstancesSize--;
    return totalInstancesSize;
  }

  private int startInstance(FakeInstance fakeInstance, int totalInstancesSize) throws Exception {
    fakeInstance.start();
    totalInstancesSize++;
    return totalInstancesSize;
  }

  private void assertEmptyCluster(HelixMirrorMakerManager helixMirrorMakerManager) {
    for (String topicName : helixMirrorMakerManager.getTopicLists()) {
      Assert.assertNull(
          helixMirrorMakerManager.getExternalViewForTopic(topicName));
      Assert.assertEquals(
          helixMirrorMakerManager.getIdealStateForTopic(topicName).getPartitionSet().size(), 0);
    }
  }
}
