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
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.controller.core.AutoTopicWhitelistingManager;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.controller.core.ManagerControllerHelix;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.uber.stream.kafka.mirrormaker.controller.core.TopicAssignmentViewBuilder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest API for topic management
 */
public class TopicManagementRestletResource extends ServerResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TopicManagementRestletResource.class);

  private final ManagerControllerHelix _managerControllerHelix;
  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final AutoTopicWhitelistingManager _autoTopicWhitelistingManager;
  private final KafkaBrokerTopicObserver _srcKafkaBrokerTopicObserver;

  public TopicManagementRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    _managerControllerHelix = (ManagerControllerHelix) getApplication().getContext()
        .getAttributes().get(ManagerControllerHelix.class.toString());
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
    _autoTopicWhitelistingManager = (AutoTopicWhitelistingManager) getApplication().getContext()
        .getAttributes().get(AutoTopicWhitelistingManager.class.toString());
    if (getApplication().getContext().getAttributes()
        .containsKey(KafkaBrokerTopicObserver.class.toString())) {
      _srcKafkaBrokerTopicObserver = (KafkaBrokerTopicObserver) getApplication().getContext()
          .getAttributes().get(KafkaBrokerTopicObserver.class.toString());
    } else {
      _srcKafkaBrokerTopicObserver = null;
    }
  }

  @Override
  @Get
  public Representation get() {
    final String topicName = (String) getRequest().getAttributes().get("topicName");
    if (topicName == null) {
      List<String> topicLists = _helixMirrorMakerManager.getTopicLists();
      if (topicLists == null || topicLists.isEmpty()) {
        return new StringRepresentation("No topic is added in MirrorMaker Controller!");
      } else {
        return new StringRepresentation(String
            .format("Current serving topics: %s", Arrays.toString(topicLists.toArray())));
      }
    }
    try {
      if (_helixMirrorMakerManager.isTopicExisted(topicName)) {
        IdealState idealStateForTopic =
            _helixMirrorMakerManager.getIdealStateForTopic(topicName);
        ExternalView externalViewForTopic =
            _helixMirrorMakerManager.getExternalViewForTopic(topicName);
        JSONObject responseJson = TopicAssignmentViewBuilder.build(
            topicName, idealStateForTopic, externalViewForTopic);
        return new StringRepresentation(responseJson.toJSONString());
      } else {
        getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation(String
            .format("Failed to get ExternalView for topic: %s, it is not existed!", topicName));
      }
    } catch (Exception e) {
      LOGGER.error("Got error during processing Get request", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(String
          .format("Failed to get ExternalView for topic: %s, with exception: %s", topicName, e));
    }
  }

  @Override
  @Post("json")
  public Representation post(Representation entity) {
    try {
      final String topicName = (String) getRequest().getAttributes().get("topicName");
      LOGGER.info("received request to whitelist topic {} on mm ", topicName);
      if (_managerControllerHelix != null) {
        // federated mode
        Form params = getRequest().getResourceRef().getQueryAsForm();
        String srcCluster = params.getFirstValue("src");
        String dstCluster = params.getFirstValue("dst");
        String routeId = params.getFirstValue("routeid");
        if (srcCluster == null || dstCluster == null || routeId == null) {
          getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
          return new StringRepresentation("Missing parameters for whitelisting topic " + topicName
              + " in federated uReplicator");
        }
        if (!_managerControllerHelix.handleTopicAssignmentEvent(topicName, srcCluster, dstCluster, routeId, "ONLINE")) {
          getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
          String resp = String.format("Failed to add new topic: %s, src=%s, dst=%s, routeid=%s",
              topicName, srcCluster, dstCluster, routeId);
          LOGGER.info(resp);
          return new StringRepresentation(resp);
        }
        String resp = String.format("Successfully add new topic: %s, src=%s, dst=%s, routeid=%s",
            topicName, srcCluster, dstCluster, routeId);
        LOGGER.info(resp);
        return new StringRepresentation(resp);
      }

      String jsonRequest = entity.getText();
      TopicPartition topicPartitionInfo = null;
      if ((jsonRequest == null || jsonRequest.isEmpty()) && topicName != null
          && _srcKafkaBrokerTopicObserver != null) {
        // Only triggered when srcKafkaObserver is there and curl call has no json blob.
        topicPartitionInfo = _srcKafkaBrokerTopicObserver.getTopicPartitionWithRefresh(topicName);
        if (topicPartitionInfo == null) {
          LOGGER.warn("failed to whitelist topic {} on mm because of not exists in src cluster", topicName);
          getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
          return new StringRepresentation(String.format(
              "Failed to add new topic: %s, it's not exsited in source kafka cluster!", topicName));
        }
      } else {
        topicPartitionInfo = TopicPartition.init(jsonRequest);
      }
      if (_autoTopicWhitelistingManager != null) {
        _autoTopicWhitelistingManager.removeFromBlacklist(topicPartitionInfo.getTopic());
      }
      if (_helixMirrorMakerManager.isTopicExisted(topicPartitionInfo.getTopic())) {
        LOGGER.info("topic {} already on mm", topicName);
        getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation(String.format(
            "Failed to add new topic: %s, it is already existed!", topicPartitionInfo.getTopic()));
      } else {
        _helixMirrorMakerManager.addTopicToMirrorMaker(topicPartitionInfo);
        LOGGER.info("successuflly whitelist the topic {}", topicName);
        return new StringRepresentation(
            String.format("Successfully add new topic: %s", topicPartitionInfo));
      }
    } catch (IOException e) {
      LOGGER.error("Got error during processing Post request", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(
          String.format("Failed to add new topic, with exception: %s", e));
    }
  }

  @Override
  @Put("json")
  public Representation put(Representation entity) {
    try {
      String jsonRequest = entity.getText();
      TopicPartition topicPartitionInfo = TopicPartition.init(jsonRequest);
      if (_autoTopicWhitelistingManager != null) {
        _autoTopicWhitelistingManager.removeFromBlacklist(topicPartitionInfo.getTopic());
      }
      if (_helixMirrorMakerManager.isTopicExisted(topicPartitionInfo.getTopic())) {
        _helixMirrorMakerManager.expandTopicInMirrorMaker(topicPartitionInfo);
        return new StringRepresentation(
            String.format("Successfully expand topic: %s", topicPartitionInfo));
      } else {
        getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation(String.format(
            "Failed to expand topic, topic: %s is not existed!", topicPartitionInfo.getTopic()));
      }
    } catch (Exception e) {
      LOGGER.error("Got error during processing Put request", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(
          String.format("Failed to expand topic, with exception: %s", e));
    }
  }

  @Override
  @Delete
  public Representation delete() {
    final String topicName = (String) getRequest().getAttributes().get("topicName");
    if (_managerControllerHelix != null) {
      // federated mode
      Form params = getRequest().getResourceRef().getQueryAsForm();
      String srcCluster = params.getFirstValue("src");
      String dstCluster = params.getFirstValue("dst");
      String routeId = params.getFirstValue("routeid");
      if (srcCluster == null || dstCluster == null || routeId == null) {
        getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        return new StringRepresentation("Missing parameters for blacklisting topic " + topicName
            + " in federated uReplicator");
      }
      if (!_managerControllerHelix.handleTopicAssignmentEvent(topicName, srcCluster, dstCluster, routeId, "OFFLINE")) {
        getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        String resp = String.format("Failed to blacklist topic: %s, src=%s, dst=%s, routeid=%s",
            topicName, srcCluster, dstCluster, routeId);
        LOGGER.info(resp);
        return new StringRepresentation(resp);
      }
      String resp = String.format("Successfully blacklisted topic: %s, src=%s, dst=%s, routeid=%s",
          topicName, srcCluster, dstCluster, routeId);
      LOGGER.info(resp);
      return new StringRepresentation(resp);
    }

    if (_autoTopicWhitelistingManager != null) {
      _autoTopicWhitelistingManager.addIntoBlacklist(topicName);
    }
    if (!_helixMirrorMakerManager.isTopicExisted(topicName)) {
      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(
          String.format("Failed to delete not existed topic: %s", topicName));
    }
    try {
      _helixMirrorMakerManager.deleteTopicInMirrorMaker(topicName);
      return new StringRepresentation(
          String.format("Successfully finished delete topic: %s", topicName));
    } catch (Exception e) {
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      LOGGER.error("Failed to delete topic: {}, with exception: {}", topicName, e);
      return new StringRepresentation(
          String.format("Failed to delete topic: %s, with exception: %s", topicName, e));
    }
  }

}
