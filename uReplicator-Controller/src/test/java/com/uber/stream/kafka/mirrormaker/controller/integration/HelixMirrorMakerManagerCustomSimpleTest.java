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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.FakeInstance;
import com.uber.stream.kafka.mirrormaker.controller.utils.ZkStarter;

public class HelixMirrorMakerManagerCustomSimpleTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HelixMirrorMakerManagerCustomSimpleTest.class);

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
    String helixClusterName = "HelixMirrorMakerManagerCustomSimpleTest";
    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setControllerPort("9090");
    controllerConf.setHelixClusterName(helixClusterName);
    controllerConf.setInstanceId("controller-0");
    controllerConf.setBackUpToGit("false");
    controllerConf.setAutoRebalanceDelayInSeconds("0");
    controllerConf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    controllerConf.setControllerMode("customized");
    HelixMirrorMakerManager helixMirrorMakerManager = new HelixMirrorMakerManager(controllerConf);
    helixMirrorMakerManager.start();
    // Adding fake workers
    LOGGER.info("Trying to add 1 instance ");
    List<FakeInstance> fakeInstances = ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR, 1, 0);
    LOGGER.info("Trying to add topic testTopic0");
    helixMirrorMakerManager.addTopicToMirrorMaker("testTopic0", 8);
    Thread.sleep(5000);

    LOGGER.info("Trying to add 1 more instances, waiting for rebalancing");
    fakeInstances.addAll(ControllerTestUtils
        .addFakeDataInstancesToAutoJoinHelixCluster(helixClusterName, ZkStarter.DEFAULT_ZK_STR, 1, 1));
    Thread.sleep(5000);
    for (int k = 0; k < 10; ++k) {
      LOGGER.info("Simulate restart nodes 1 by 1");
    }
    int totalInstancesSize = fakeInstances.size();
    for (int i = 0; i < totalInstancesSize * 2; ++i) {
      if (i % 2 == 0) {
        for (int k = 0; k < 10; ++k) {
          LOGGER.info("Trying to bring down: " + fakeInstances.get(i / 2).getInstanceId());
        }
        fakeInstances.get(i / 2).stop();
      } else {
        for (int k = 0; k < 10; ++k) {
          LOGGER.info("Trying to bring up: " + fakeInstances.get(i / 2).getInstanceId());
        }
        fakeInstances.get(i / 2).start();
      }
      Thread.sleep(30000);
    }

    helixMirrorMakerManager.stop();
  }

}
