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
package com.uber.stream.kafka.mirrormaker.controller.integration;

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

import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.FakeInstance;
import com.uber.stream.kafka.mirrormaker.controller.utils.ZkStarter;

public class HelixMirrorMakerManagerCustomEmptyWorkerTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HelixMirrorMakerManagerCustomEmptyWorkerTest.class);

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
    String helixClusterName = "HelixMirrorMakerManagerCustomEmptyWorkerTest";
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerPort("9090");
    controllerConf.setHelixClusterName(helixClusterName);
    controllerConf.setInstanceId("controller-0");
    controllerConf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    controllerConf.setControllerMode("customized");
    controllerConf.setBackUpToGit("false");
    controllerConf.setAutoRebalanceDelayInSeconds("0");
    HelixMirrorMakerManager helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();
    LOGGER.info("Trying to add topic testTopic0");
    helixMirrorMakerManager.addTopicToMirrorMaker("testTopic0", 8);
    Thread.sleep(1000);
    // Adding fake workers
    LOGGER.info("Trying to add 1 instance ");
    List<FakeInstance> fakeInstances = ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR, 1,
            0);
    Thread.sleep(5000);
    assertTopicExternalViewWithGivenPartitions(helixMirrorMakerManager, "testTopic0", 8, 1);
    LOGGER.info("Trying to add 1 more instances, waiting for rebalancing");
    fakeInstances.addAll(ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR, 1,
            1));
    Thread.sleep(5000);
    assertTopicExternalViewWithGivenPartitions(helixMirrorMakerManager, "testTopic0", 8, 2);
    helixMirrorMakerManager.stop();
  }

  private void assertTopicExternalViewWithGivenPartitions(
      HelixMirrorMakerManager helixMirrorMakerManager, String topicName, int partitions,
      int numInstances) {
    ExternalView externalViewForTopic = helixMirrorMakerManager.getExternalViewForTopic(topicName);
    LOGGER.info("ExternalView: " + externalViewForTopic.toString());
    Assert.assertEquals(externalViewForTopic.getPartitionSet().size(), partitions);
    Map<String, Integer> serverToPartitionMapping = new HashMap<String, Integer>();
    for (String partition : externalViewForTopic.getPartitionSet()) {
      String instanceName = externalViewForTopic.getStateMap(partition).keySet().iterator().next();
      if (!serverToPartitionMapping.containsKey(instanceName)) {
        serverToPartitionMapping.put(instanceName, 0);
      }
      serverToPartitionMapping.put(instanceName, serverToPartitionMapping.get(instanceName) + 1);
    }
    int expectedLowerBound = (int) Math.floor((double) partitions / (double) numInstances);
    int expectedUpperBound = (int) Math.ceil((double) partitions / (double) numInstances);
    for (String instanceName : serverToPartitionMapping.keySet()) {
      // May not be perfect balancing.
      Assert.assertTrue(serverToPartitionMapping.get(instanceName) >= expectedLowerBound - 1);
      Assert.assertTrue(serverToPartitionMapping.get(instanceName) <= expectedUpperBound + 1);
    }
  }
}
