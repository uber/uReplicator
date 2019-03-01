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

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.Constants;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import org.apache.commons.lang.StringUtils;
import org.restlet.data.Form;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;

import java.util.Set;

public class TopicPartitionBlacklistRestletResource extends ServerResource {
  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public TopicPartitionBlacklistRestletResource() {
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  /**
   * Gets topic partition in blacklist
   *
   * @return topic partition in blacklist
   */
  @Override
  @Get
  public Representation get() {
    Set<TopicPartition> blacklist = _helixMirrorMakerManager.getTopicPartitionBlacklist();
    JSONObject result = new JSONObject();
    result.put("blacklist", blacklist);
    return new StringRepresentation(result.toJSONString());
  }

  /**
   * blacklist or whitelist topic partition
   *
   * @return status
   */
  @Post
  public Representation post() {
    Form params = getRequest().getResourceRef().getQueryAsForm();
    String topicName = params.getFirstValue("topic");
    String partitionName = params.getFirstValue("partition");
    String opt = params.getFirstValue("opt");
    if (StringUtils.isEmpty(topicName) || StringUtils.isEmpty(partitionName) || StringUtils.isEmpty(opt)) {
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("Parameter topic, partition and opt are required.");
    }

    try {
      int partition = Integer.parseInt(partitionName);

      if ("blacklist".equalsIgnoreCase(opt)) {
        _helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker(topicName, partition, Constants.HELIX_OFFLINE_STATE);
      } else if ("whitelist".equalsIgnoreCase(opt)) {
        _helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker(topicName, partition, Constants.HELIX_ONLINE_STATE);
      } else {
        getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        return new StringRepresentation("Invalid parameter opt");
      }
    } catch (NumberFormatException e) {
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("Parameter partition should be Integer");
    } catch (IllegalArgumentException e) {
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(e.getMessage());
    } catch (Exception e) {
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(e.getMessage());
    }
    return new StringRepresentation("OK");
  }
}
