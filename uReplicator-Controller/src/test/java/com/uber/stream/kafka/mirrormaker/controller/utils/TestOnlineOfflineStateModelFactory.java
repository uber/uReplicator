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

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class TestOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  int _delay;
  final String _instanceId;

  public TestOnlineOfflineStateModelFactory(String instanceId, int delay) {
    _delay = delay;
    _instanceId = instanceId;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String stateUnitKey) {
    TestOnlineOfflineStateModel stateModel = new TestOnlineOfflineStateModel(_instanceId);
    stateModel.setDelay(_delay);
    return stateModel;
  }

  public static class TestOnlineOfflineStateModel extends StateModel {
    int _transDelay = 0;
    final String _instanceId;

    public TestOnlineOfflineStateModel(String instanceId) {
      _instanceId = instanceId;
    }

    public void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      System.out.println("TestOnlineOfflineStateModel.onBecomeOnlineFromOffline for topic: "
          + message.getResourceName() + ", partition: " + message.getPartitionName()
          + " to instance: " + _instanceId);
      sleep();
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println("TestOnlineOfflineStateModel.onBecomeOfflineFromOnline for topic: "
          + message.getResourceName() + ", partition: " + message.getPartitionName()
          + " to instance: " + _instanceId);
      sleep();
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      System.out.println("TestOnlineOfflineStateModel.onBecomeDroppedFromOffline() for topic: "
          + message.getResourceName() + ", partition: " + message.getPartitionName()
          + " to instance: " + _instanceId);
      sleep();
    }

    private void sleep() {
      try {
        Thread.sleep(_transDelay);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
