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
package com.uber.stream.kafka.mirrormaker.controller.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.mirrormaker.controller.core.OnlineOfflineStateModel;

/**
 * HelixSetupUtils handles how to create or get a helixCluster in controller.
 */
public class HelixSetupUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixSetupUtils.class);

  public static synchronized HelixManager setup(String helixClusterName, String zkPath,
      String controllerInstanceId) {
    try {
      createHelixClusterIfNeeded(helixClusterName, zkPath);
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
      return null;
    }

    try {
      return startHelixControllerInStandadloneMode(helixClusterName, zkPath, controllerInstanceId);
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
      return null;
    }
  }

  public static void createHelixClusterIfNeeded(String helixClusterName, String zkPath) {
    final HelixAdmin admin = new ZKHelixAdmin(zkPath);

    if (admin.getClusters().contains(helixClusterName)) {
      LOGGER.info(
          "cluster already exist, skipping it.. ********************************************* ");
      return;
    }

    LOGGER.info("Creating a new cluster, as the helix cluster : " + helixClusterName
        + " was not found ********************************************* ");
    admin.addCluster(helixClusterName, false);

    LOGGER.info("Enable mirror maker machines auto join.");
    final HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER)
        .forCluster(helixClusterName).build();

    final Map<String, String> props = new HashMap<String, String>();
    props.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    props.put(MessageType.STATE_TRANSITION + "." + HelixTaskExecutor.MAX_THREADS,
        String.valueOf(100));

    admin.setConfig(scope, props);

    LOGGER.info("Adding state model definition named : OnlineOffline generated using : "
        + OnlineOfflineStateModel.class.toString()
        + " ********************************************** ");

    // add state model definition
    admin.addStateModelDef(helixClusterName, "OnlineOffline", OnlineOfflineStateModel.build());
    LOGGER.info("New Cluster setup completed... ********************************************** ");
  }

  private static HelixManager startHelixControllerInStandadloneMode(String helixClusterName,
      String zkUrl, String controllerInstanceId) {
    LOGGER.info("Starting Helix Standalone Controller ... ");
    return HelixControllerMain.startHelixController(zkUrl, helixClusterName, controllerInstanceId,
        HelixControllerMain.STANDALONE);
  }
}
