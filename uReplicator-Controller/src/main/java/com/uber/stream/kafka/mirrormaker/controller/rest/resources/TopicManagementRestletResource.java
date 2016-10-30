package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.uber.stream.kafka.mirrormaker.controller.core.AutoTopicWhitelistingManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.controller.core.TopicPartition;

/**
 * Rest API for topic management
 */
public class TopicManagementRestletResource extends ServerResource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TopicManagementRestletResource.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final AutoTopicWhitelistingManager _autoTopicWhitelistingManager;
  private final KafkaBrokerTopicObserver _srcKafkaBrokerTopicObserver;

  public TopicManagementRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
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
        JSONObject responseJson = new JSONObject();
        responseJson.put("topic", topicName);
        JSONObject externalViewPartitionToServerMappingJson = new JSONObject();
        for (String partition : externalViewForTopic.getPartitionSet()) {
          Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
          for (String server : stateMap.keySet()) {
            if (!externalViewPartitionToServerMappingJson.containsKey(partition)) {
              externalViewPartitionToServerMappingJson.put(partition, new JSONArray());
            }
            externalViewPartitionToServerMappingJson.getJSONArray(partition).add(server);
          }
        }
        responseJson.put("externalView", externalViewPartitionToServerMappingJson);

        JSONObject idealStatePartitionToServerMappingJson = new JSONObject();
        for (String partition : idealStateForTopic.getPartitionSet()) {
          Map<String, String> stateMap = idealStateForTopic.getInstanceStateMap(partition);
          if (stateMap != null) {
            for (String server : stateMap.keySet()) {
              if (!idealStatePartitionToServerMappingJson.containsKey(partition)) {
                idealStatePartitionToServerMappingJson.put(partition, new JSONArray());
              }
              idealStatePartitionToServerMappingJson.getJSONArray(partition).add(server);
            }
          }
        }
        responseJson.put("idealState", idealStatePartitionToServerMappingJson);
        Map<String, List<String>> serverToPartitionMapping = new HashMap<String, List<String>>();
        JSONObject serverToPartitionMappingJson = new JSONObject();
        JSONObject serverToNumPartitionsMappingJson = new JSONObject();

        for (String partition : externalViewForTopic.getPartitionSet()) {
          Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
          for (String server : stateMap.keySet()) {
            if (stateMap.get(server).equals("ONLINE")) {
              if (!serverToPartitionMapping.containsKey(server)) {
                serverToPartitionMapping.put(server, new ArrayList<String>());
                serverToPartitionMappingJson.put(server, new JSONArray());
                serverToNumPartitionsMappingJson.put(server, 0);
              }
              serverToPartitionMapping.get(server).add(partition);
              serverToPartitionMappingJson.getJSONArray(server).add(partition);
              serverToNumPartitionsMappingJson.put(server,
                  serverToNumPartitionsMappingJson.getInteger(server) + 1);
            }
          }
        }
        responseJson.put("serverToPartitionMapping", serverToPartitionMappingJson);
        responseJson.put("serverToNumPartitionsMapping", serverToNumPartitionsMappingJson);
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
      String jsonRequest = entity.getText();
      TopicPartition topicPartitionInfo = null;
      if ((jsonRequest == null || jsonRequest.isEmpty()) && topicName != null
          && _srcKafkaBrokerTopicObserver != null) {
        // Only triggered when srcKafkaObserver is there and curl call has no json blob.
        topicPartitionInfo = _srcKafkaBrokerTopicObserver.getTopicPartition(topicName);
        if (topicPartitionInfo == null) {
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
        getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation(String.format(
            "Failed to add new topic: %s, it is already existed!", topicPartitionInfo.getTopic()));
      } else {
        _helixMirrorMakerManager.addTopicToMirrorMaker(topicPartitionInfo);
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
    } catch (IOException e) {
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
