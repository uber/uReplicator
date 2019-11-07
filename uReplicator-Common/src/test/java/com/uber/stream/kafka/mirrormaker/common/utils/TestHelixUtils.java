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
package com.uber.stream.kafka.mirrormaker.common.utils;

import com.uber.stream.kafka.mirrormaker.common.Constants;
import java.util.Arrays;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @Author: Tboy
 */
public class TestHelixUtils {


  private HelixManager _helixZkManager;

  @BeforeTest
  public void before() {
    ZkStarter.startLocalZkServer();
    _helixZkManager = HelixSetupUtils
        .setup("TestHelixUtils", ZkStarter.DEFAULT_ZK_STR, "testInstance1");
    try {
      _helixZkManager.connect();
    } catch (Exception e) {
      e.getStackTrace();
    }
  }

  @Test
  public void testUpdateClusterConfig() {
    HelixUtils.updateClusterConfig(_helixZkManager, Constants.AUTO_BALANCING, Constants.DISABLE);
    HelixConfigScope scope = HelixUtils.newClusterConfigScope(_helixZkManager);
    Map<String, String> config = _helixZkManager.getClusterManagmentTool()
        .getConfig(scope, Arrays.asList(Constants.AUTO_BALANCING));
    Assert.assertTrue(config != null);
    Assert.assertTrue(config.containsKey(Constants.AUTO_BALANCING));
    Assert.assertTrue(config.get(Constants.AUTO_BALANCING).equalsIgnoreCase(Constants.DISABLE));
  }

  @Test
  public void testIsClusterConfigEnabled() {
    boolean autoScaling = HelixUtils
        .isClusterConfigEnabled(_helixZkManager, Constants.AUTO_SCALING);
    Assert.assertTrue(autoScaling);

    HelixUtils.updateClusterConfig(_helixZkManager, Constants.AUTO_BALANCING, Constants.DISABLE);
    boolean autoBalancing = HelixUtils
        .isClusterConfigEnabled(_helixZkManager, Constants.AUTO_BALANCING);
    Assert.assertTrue(!autoBalancing);

  }

  @Test
  public void testIsClusterConfigEnabled3Params() {
    boolean autoScaling = HelixUtils
        .isClusterConfigEnabled(_helixZkManager, "uReplicator", true);
    Assert.assertTrue(autoScaling);

    HelixUtils.updateClusterConfig(_helixZkManager, "uReplicator", Constants.DISABLE);
    boolean isUreplicator = HelixUtils
        .isClusterConfigEnabled(_helixZkManager, "uReplicator", true);
    Assert.assertTrue(!isUreplicator);
  }
}
