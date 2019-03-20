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
package com.uber.stream.kafka.mirrormaker.controller.core;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State machine model factory for Controller in Manager-Controller Helix.
 * @author Zhenmin Li
 */
public class ControllerStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStateModelFactory.class);

  private static final String SEPARATOR = "@";

  private final ManagerControllerHelix _controllerHelix;

  public ControllerStateModelFactory(ManagerControllerHelix controllerHelix) {
    this._controllerHelix = controllerHelix;
  }

  @Override
  public StateModel createNewStateModel(String stateUnitKey) {
    ControllerStateModel stateModel = new ControllerStateModel(_controllerHelix);
    return stateModel;
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE','DROPPED'}", initialState = "OFFLINE")
  public static class ControllerStateModel extends StateModel {
    private final ManagerControllerHelix _controllerHelix;

    public ControllerStateModel(ManagerControllerHelix controllerHelix) {
      this._controllerHelix = controllerHelix;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      LOGGER.info("ControllerStateModel.onBecomeOnlineFromOffline() for resource: "
        + message.getResourceName() + ", partition: " + message.getPartitionName());
      handleStateChange(message);
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.info("ControllerStateModel.onBecomeOfflineFromOnline() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      handleStateChange(message);
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("ControllerStateModel.onBecomeDroppedFromOffline() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      handleStateChange(message);
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      LOGGER.info("ControllerStateModel.onBecomeOfflineFromError() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      handleStateChange(message);
    }

    @Transition(from = "ERROR", to = "DROPPED")
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      LOGGER.info("ControllerStateModel.onBecomeDroppedFromError() for resource: "
          + message.getResourceName() + ", partition: " + message.getPartitionName());
      handleStateChange(message);
    }

    private void handleStateChange(Message message) {
      if (message.getResourceName().startsWith(SEPARATOR)) {
        String[] srcDest = message.getResourceName().split(SEPARATOR);
        if (srcDest.length == 3) {
          _controllerHelix.handleRouteAssignmentEvent(srcDest[1], srcDest[2], message.getPartitionName(), message.getToState());
        } else {
          String msg = String.format("Invalid resource name for route: resource=%s, partition=%s",
              message.getResourceName(), message.getPartitionName());
          LOGGER.error(msg);
          throw new IllegalArgumentException(msg);
        }
      } else {
        String[] srcDest = message.getPartitionName().split(SEPARATOR);
        if (srcDest.length == 4) {
          _controllerHelix.handleTopicAssignmentEvent(message.getResourceName(), srcDest[1], srcDest[2], srcDest[3], message.getToState());
        } else {
          String msg = String.format("Invalid partition name for topic: resource=%s, partition=%s",
              message.getResourceName(), message.getPartitionName());
          LOGGER.error(msg);
          throw new IllegalArgumentException(msg);
        }
      }
    }
  }
}
