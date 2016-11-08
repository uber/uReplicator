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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FakeInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(FakeInstance.class);
  private HelixManager _helixZkManager;
  private final String _helixClusterName;
  private final String _instanceId;
  private final String _zkServer;

  public FakeInstance(String helixClusterName, String instanceId, String zkServer) {
    _helixClusterName = helixClusterName;
    _instanceId = instanceId;
    _zkServer = zkServer;
  }

  public void start() throws Exception {
    _helixZkManager = HelixManagerFactory.getZKHelixManager(_helixClusterName, _instanceId,
        InstanceType.PARTICIPANT, _zkServer);
    StateMachineEngine stateMachineEngine = _helixZkManager.getStateMachineEngine();
    // create a stateModelFactory that returns a statemodel object for each partition.
    TestOnlineOfflineStateModelFactory stateModelFactory =
        new TestOnlineOfflineStateModelFactory(_instanceId, 0);
    stateMachineEngine.registerStateModelFactory("OnlineOffline", stateModelFactory);
    _helixZkManager.connect();
  }

  public void stop() throws Exception {
    LOGGER.info(String.format("Shutdown fakeInstance: %s", _instanceId));
    _helixZkManager.disconnect();
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String toString() {
    return String.format("FakeInstance: %s", _instanceId);
  }

  public static class ShutdownHook extends Thread {
    private final FakeInstance _fakeInstance;

    public ShutdownHook(FakeInstance fakeInstance) {
      _fakeInstance = fakeInstance;
    }

    @Override
    public void run() {
      LOGGER.info("Running shutdown hook");
      if (_fakeInstance != null) {
        try {
          _fakeInstance.stop();
        } catch (Exception e) {
          LOGGER.error("Failed to Shutdown FakeInstance !!");
        }
      }
      LOGGER.info("Shutdown completed !!");
    }
  }

  public static FakeInstance startDefault(String instanceId) throws Exception {
    String zkPath = "localhost:2181";
    String helixClusterName = "testMirrorMaker";
    FakeInstance fakeInstance = new FakeInstance(helixClusterName, instanceId, zkPath);
    fakeInstance.start();

    final ShutdownHook shutdownHook = new ShutdownHook(fakeInstance);
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    return fakeInstance;
  }

  public static void main(String[] args) throws Exception {
    // You can also specify your own instanceId
    // String instanceId = "testHelixMirrorMaker01";
    String instanceId = "testHelixMirrorMaker-" + System.currentTimeMillis();
    startDefault(instanceId);
    while (true) {
      Thread.sleep(10000);
    }

  }
}
