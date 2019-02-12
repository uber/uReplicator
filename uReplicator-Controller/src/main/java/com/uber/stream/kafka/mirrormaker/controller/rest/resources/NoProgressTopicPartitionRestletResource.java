/*
 * Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.OffsetMonitor;
import kafka.common.TopicAndPartition;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.restlet.data.Form;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import java.util.Map;


public class NoProgressTopicPartitionRestletResource extends ServerResource {
  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public NoProgressTopicPartitionRestletResource() {
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    JSONObject responseJson = new JSONObject();
    OffsetMonitor offsetMonitor = _helixMirrorMakerManager.getOffsetMonitor();
    if (offsetMonitor.getNoProgressTopicToOffsetMap() == null || offsetMonitor.getNoProgressTopicToOffsetMap().keySet().size() == 0) {
      return new StringRepresentation(responseJson.toJSONString());
    }
    JSONArray jsonArray = new JSONArray();
    for (TopicAndPartition info : offsetMonitor.getNoProgressTopicToOffsetMap().keySet()) {

      JSONObject node = new JSONObject();
      node.put("topic", info.topic());
      node.put("partition", info.partition());

      IdealState idealStateForTopic =
          _helixMirrorMakerManager.getIdealStateForTopic(info.topic());
      Map<String, String> idealStateMap = idealStateForTopic.getInstanceStateMap(String.valueOf(info.partition()));
      ExternalView externalViewForTopic =
          _helixMirrorMakerManager.getExternalViewForTopic(info.topic());
      Map<String, String> stateMap = externalViewForTopic.getStateMap(String.valueOf(info.partition()));
      if (idealStateMap != null && idealStateMap.keySet().size() != 0) {
        node.put("idealWorker", idealStateMap.keySet().iterator().next());
      }
      if (stateMap != null && stateMap.keySet().size() != 0) {
        node.put("actualWorker", stateMap.keySet().iterator().next());
      }
      jsonArray.add(node);

    }
    responseJson.put("topics", jsonArray);

    return new StringRepresentation(responseJson.toJSONString());
  }
}
