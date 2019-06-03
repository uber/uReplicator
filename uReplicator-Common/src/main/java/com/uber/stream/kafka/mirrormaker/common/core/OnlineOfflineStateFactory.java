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
package com.uber.stream.kafka.mirrormaker.common.core;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnlineOfflineStateFactory extends StateModelFactory<StateModel> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(OnlineOfflineStateFactory.class);
  public static final String STATE_MODE_DEF = "OnlineOffline";
  private final HelixHandler helixHandler;

  public OnlineOfflineStateFactory(HelixHandler helixHandler) {
    this.helixHandler = helixHandler;
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String partitionName) {
    OnlineOfflineStateModelHandler stateModel = new OnlineOfflineStateModelHandler(helixHandler);
    return stateModel;
  }

  protected class OnlineOfflineStateModelHandler extends StateModel {

    private final HelixHandler helixHandler;

    public OnlineOfflineStateModelHandler(HelixHandler helixHandler) {
      this.helixHandler = helixHandler;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.info("OnlineOfflineStateModel.onBecomeOnlineFromOffline() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      helixHandler.onBecomeOnlineFromOffline(message);
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.info("OnlineOfflineStateModel.onBecomeOfflineFromOnline() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      helixHandler.onBecomeOfflineFromOnline(message);
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("OnlineOfflineStateModel.onBecomeDroppedFromOffline() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      helixHandler.onBecomeDroppedFromOffline(message);
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      LOGGER.info("OnlineOfflineStateModel.onBecomeOfflineFromError() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      helixHandler.onBecomeOfflineFromError(message);
    }

    @Transition(from = "ERROR", to = "DROPPED")
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      LOGGER.info("OnlineOfflineStateModel.onBecomeDroppedFromError() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      helixHandler.onBecomeDroppedFromError(message);
    }
  }

}
