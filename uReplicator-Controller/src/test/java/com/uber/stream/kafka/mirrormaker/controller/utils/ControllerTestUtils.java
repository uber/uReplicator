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
package com.uber.stream.kafka.mirrormaker.controller.utils;

import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ControllerTestUtils {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ControllerTestUtils.class);

  public static List<FakeInstance> addFakeDataInstancesToAutoJoinHelixCluster(
      String helixClusterName, String zkServer, int numInstances, int base) throws Exception {
    List<FakeInstance> ret = new ArrayList<FakeInstance>();
    for (int i = base; i < numInstances + base; ++i) {
      final String instanceId = "Server_localhost_" + i;
      FakeInstance fakeInstance = new FakeInstance(helixClusterName, instanceId, zkServer);
      fakeInstance.start();
      ret.add(fakeInstance);
    }
    return ret;
  }

  public static ControllerConf initControllerConf(String clusterName) {
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerPort("9090");
    controllerConf.setHelixClusterName(clusterName);
    controllerConf.setDeploymentName("Deployment" + clusterName);
    controllerConf.setInstanceId("controller-0");
    controllerConf.setControllerMode("customized");
    controllerConf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    controllerConf.setBackUpToGit("false");
    controllerConf.setAutoRebalanceDelayInSeconds("1");
    controllerConf.setWorkloadRefreshPeriodInSeconds("5");
    return controllerConf;
  }

  public static Map<String, Integer> assertServerPartitionCount(
      HelixMirrorMakerManager helixMirrorMakerManager,
      int numTotalPartitions) {
    Map<String, Integer> serverToPartitionMapping = new HashMap<>();
    int assginedPartitions = 0;
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
        assginedPartitions++;
      }
    }
    Assert.assertEquals(assginedPartitions, numTotalPartitions, "assignedPartitions not match with numTotalPartitions");
    return serverToPartitionMapping;
  }

  public static Map<String, Integer> assertIdealServerPartitionCount(
      HelixMirrorMakerManager helixMirrorMakerManager,
      int numTotalPartitions) {
    Map<String, Integer> serverToPartitionMapping = new HashMap<>();
    int assginedPartitions = 0;
    for (String topicName : helixMirrorMakerManager.getTopicLists()) {
      IdealState idealStateForTopic =
          helixMirrorMakerManager.getIdealStateForTopic(topicName);
      LOGGER.info("IdealState: " + idealStateForTopic.toString());
      for (String partition : idealStateForTopic.getPartitionSet()) {
        String instanceName =
            idealStateForTopic.getInstanceStateMap(partition).keySet().iterator().next();
        if (!serverToPartitionMapping.containsKey(instanceName)) {
          serverToPartitionMapping.put(instanceName, 0);
        }
        serverToPartitionMapping.put(instanceName, serverToPartitionMapping.get(instanceName) + 1);
        assginedPartitions++;
      }
    }
    Assert.assertEquals(assginedPartitions, numTotalPartitions, "assignedPartitions not match with numTotalPartitions");
    return serverToPartitionMapping;
  }

  public static void assertInstanceOwnedTopicPartitionsBalanced(HelixMirrorMakerManager helixMirrorMakerManager,
      int numInstances,
      int numTotalPartitions) throws InterruptedException {
    int maxRetryCount = 15;

    for (int i = 0; i < maxRetryCount; i ++) {
      try {
        if (assertInstanceOwnedTopicPartitionsBalancedOnce(helixMirrorMakerManager, numInstances, numTotalPartitions)) {
          return;
        }
      } catch (AssertionError e) {
        LOGGER.info("assertInstanceOwnedTopicPartitionsBalanced, try in next 1000ms, retry count: {}", i);
        Thread.sleep(1000);
      }
    }
    assertInstanceOwnedTopicPartitionsBalancedOnce(helixMirrorMakerManager, numInstances, numTotalPartitions);
  }

  public static boolean assertInstanceOwnedTopicPartitionsBalancedOnce(
      HelixMirrorMakerManager helixMirrorMakerManager,
      int numInstances,
      int numTotalPartitions) {
    assertIdealServerPartitionCount(helixMirrorMakerManager, numTotalPartitions);
    Map<String, Integer> serverToPartitionMapping = assertServerPartitionCount(helixMirrorMakerManager, numTotalPartitions);
    int expectedLowerBound = (int) Math.floor((double) numTotalPartitions / (double) numInstances);
    int expectedUpperBound = (int) Math.ceil((double) numTotalPartitions / (double) numInstances);
    for (String instanceName : serverToPartitionMapping.keySet()) {
      // May not be perfect balancing.
      LOGGER.info("Current {} serving {} partitions, expected [{}, {}]", instanceName,
          serverToPartitionMapping.get(instanceName), expectedLowerBound,
          expectedUpperBound);
      int serverPartitions = serverToPartitionMapping.get(instanceName);
      Assert.assertTrue(serverPartitions >= expectedLowerBound, String.format("serverPartitions %d less than expected lower bound %d", serverPartitions, expectedLowerBound));
      Assert.assertTrue(serverPartitions <= expectedUpperBound, String.format("serverPartitions %d greater than expected upper bound %d", serverPartitions, expectedUpperBound));
    }
    return true;
  }
}
