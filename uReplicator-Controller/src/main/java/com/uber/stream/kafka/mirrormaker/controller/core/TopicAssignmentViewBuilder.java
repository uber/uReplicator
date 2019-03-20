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

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.Constants;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TopicAssignmentViewBuilder {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TopicAssignmentViewBuilder.class);

  private TopicAssignmentViewBuilder() {
  }

  public static JSONObject build(String topicName, IdealState idealStateForTopic, ExternalView externalViewForTopic) {
    JSONObject responseJson = new JSONObject();
    responseJson.put("topic", topicName);
    responseJson.put("externalView", buildExternalView(topicName, externalViewForTopic));
    responseJson.put("idealState", buildIdealState(topicName, idealStateForTopic));

    JSONObject serverToPartitionMappingJson = new JSONObject();
    JSONObject serverToNumPartitionsMappingJson = new JSONObject();

    if (externalViewForTopic != null) {
      for (String partition : externalViewForTopic.getPartitionSet()) {
        Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
        for (String server : stateMap.keySet()) {
          String state = stateMap.get(server);
          if (!serverToPartitionMappingJson.containsKey(server)) {
            serverToPartitionMappingJson.put(server, new JSONObject());
            serverToNumPartitionsMappingJson.put(server, 0);
          }
          serverToPartitionMappingJson.getJSONObject(server).put(partition, stateMap.get(server));
          if (state.equalsIgnoreCase(Constants.HELIX_ONLINE_STATE)) {
            serverToNumPartitionsMappingJson.put(server,
                serverToNumPartitionsMappingJson.getInteger(server) + 1);
          }
        }
      }
    }
    responseJson.put("serverToPartitionMapping", serverToPartitionMappingJson);
    responseJson.put("serverToNumPartitionsMapping", serverToNumPartitionsMappingJson);

    return responseJson;
  }

  private static JSONObject buildExternalView(String topicName, ExternalView externalViewForTopic) {
    JSONObject externalViewPartitionToServerMappingJson = new JSONObject();
    if (externalViewForTopic == null) {
      LOGGER.info("External view for topic " + topicName + " is NULL");
    } else {
      for (String partition : externalViewForTopic.getPartitionSet()) {
        Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
        for (String server : stateMap.keySet()) {
          if (!externalViewPartitionToServerMappingJson.containsKey(partition)) {
            externalViewPartitionToServerMappingJson.put(partition, new JSONObject());
          }
          externalViewPartitionToServerMappingJson.getJSONObject(partition).put(server, stateMap.get(server));
        }
      }
    }
    return externalViewPartitionToServerMappingJson;
  }

  private static JSONObject buildIdealState(String topicName, IdealState idealStateForTopic) {
    JSONObject idealStatePartitionToServerMappingJson = new JSONObject();
    if (idealStateForTopic == null) {
      LOGGER.info("Ideal state for topic " + topicName + " is NULL");
    } else {
      for (String partition : idealStateForTopic.getPartitionSet()) {
        Map<String, String> stateMap = idealStateForTopic.getInstanceStateMap(partition);
        if (stateMap != null) {
          for (String server : stateMap.keySet()) {
            if (!idealStatePartitionToServerMappingJson.containsKey(partition)) {
              idealStatePartitionToServerMappingJson.put(partition, new JSONObject());
            }
            idealStatePartitionToServerMappingJson.getJSONObject(partition).put(server, stateMap.get(server));
          }
        }
      }
    }
    return idealStatePartitionToServerMappingJson;
  }
}
