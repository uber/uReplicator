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
package com.uber.stream.kafka.mirrormaker.controller.core;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.StateModelDefinition;

/**
 * Helix built-in Online-offline state model definition
 */
public final class OnlineOfflineStateModel extends StateModelDefinition {
  public static final String name = "OnlineOffline";

  public enum States {
    ONLINE,
    OFFLINE
  }

  public OnlineOfflineStateModel() {
    super(generateConfigForOnlineOffline());
  }

  /**
   * Build OnlineOffline state model definition
   * @return
   */
  public static StateModelDefinition build() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(name);
    // init state
    builder.initialState(States.OFFLINE.name());

    // add states
    builder.addState(States.ONLINE.name(), 20);
    builder.addState(States.OFFLINE.name(), -1);
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name(), -1);
    }

    // add transitions

    builder.addTransition(States.ONLINE.name(), States.OFFLINE.name(), 25);
    builder.addTransition(States.OFFLINE.name(), States.ONLINE.name(), 5);
    builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name(), 0);

    // bounds
    builder.dynamicUpperBound(States.ONLINE.name(), "R");

    return builder.build();
  }

  /**
   * Generate OnlineOffline state model definition
   * Replaced by OnlineOfflineSMD#build()
   * @return
   */
  @Deprecated
  public static ZNRecord generateConfigForOnlineOffline() {
    ZNRecord record = new ZNRecord("OnlineOffline");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("ONLINE");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("ONLINE")) {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      if (state.equals("DROPPED")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }

    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals("ONLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "ONLINE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("OFFLINE-ONLINE");
    stateTransitionPriorityList.add("ONLINE-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");

    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
  }

}
