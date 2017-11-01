package com.uber.stream.kafka.mirrormaker.manager.rest.resources;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest API for topic management
 */
public class TopicManagementRestletResource extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicManagementRestletResource.class);

  private static final String SEPARATOR = "@";

  private final ControllerHelixManager _helixMirrorMakerManager;
  private final KafkaBrokerTopicObserver _srcKafkaBrokerTopicObserver;

  public TopicManagementRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);

    _helixMirrorMakerManager = (ControllerHelixManager) getApplication().getContext()
        .getAttributes().get(ControllerHelixManager.class.toString());

    // TODO: _srcKafkaBrokerTopicObserver is null
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
        return new StringRepresentation("No topic is added in uReplicator!\n");
      } else {
        return new StringRepresentation(String
            .format("Current serving topics: %s\n", Arrays.toString(topicLists.toArray())));
      }
    }
    // pipeline startsWith "@"
    if (topicName.startsWith("@")) {
      try {
        if (_helixMirrorMakerManager.isPipelineExisted(topicName)) {
          IdealState idealStateForTopic =
              _helixMirrorMakerManager.getIdealStateForTopic(topicName);
          ExternalView externalViewForTopic =
              _helixMirrorMakerManager.getExternalViewForTopic(topicName);
          JSONObject responseJson = new JSONObject();
          responseJson.put("topic", topicName);
          JSONObject externalViewPartitionToServerMappingJson = new JSONObject();
          if (externalViewForTopic == null) {
            LOGGER.info("External view for topic " + topicName + " is NULL");
          } else {
            for (String partition : externalViewForTopic.getPartitionSet()) {
              Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
              for (String server : stateMap.keySet()) {
                if (!externalViewPartitionToServerMappingJson.containsKey(partition)) {
                  externalViewPartitionToServerMappingJson.put(partition, new JSONArray());
                }
                externalViewPartitionToServerMappingJson.getJSONArray(partition).add(server);
              }
            }
          }
          responseJson.put("externalView", externalViewPartitionToServerMappingJson);

          JSONObject idealStatePartitionToServerMappingJson = new JSONObject();
          if (idealStateForTopic == null) {
            LOGGER.info("Ideal state for topic " + topicName + " is NULL");
          } else {
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
          }
          responseJson.put("idealState", idealStatePartitionToServerMappingJson);
          Map<String, List<String>> serverToPartitionMapping = new HashMap<String, List<String>>();
          JSONObject serverToPartitionMappingJson = new JSONObject();
          JSONObject serverToNumPartitionsMappingJson = new JSONObject();

          if (externalViewForTopic != null) {
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
    if (_helixMirrorMakerManager.isTopicExisted(topicName)) {
      return new StringRepresentation(String
          .format("Found topic: %s\n", _helixMirrorMakerManager.getTopic(topicName)));
    } else {
      return new StringRepresentation(String
          .format("Failed to find topic: %s\n", topicName));
    }
  }

  @Override
  @Post
  public Representation post(Representation entity) {
    final String topicName = (String) getRequest().getAttributes().get("topicName");
    Form queryParams = getRequest().getResourceRef().getQueryAsForm();
    String srcCluster = queryParams.getFirstValue("src");
    String dstCluster = queryParams.getFirstValue("dst");

    LOGGER.info("Received request to whitelist topic {} from {} to {} on uReplicator ",
        topicName, srcCluster, dstCluster);
    // TODO: _srcKafkaBrokerTopicObserver is null
    // TopicPartition topicPartitionInfo = _srcKafkaBrokerTopicObserver.getTopicPartitionWithRefresh(topicName);
    TopicPartition topicPartitionInfo = new TopicPartition(topicName, 4);
    if (topicPartitionInfo == null) {
      LOGGER.warn("Failed to whitelist topic {} on uReplicator because of not exists in src cluster", topicName);
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(
          String.format("Failed to whitelist new topic: %s, it's not existed in source Kafka cluster!\n", topicName));
    }
    // TODO: validate src->dst combination
    String pipeline = SEPARATOR + srcCluster + SEPARATOR + dstCluster;
    if (_helixMirrorMakerManager.isTopicPipelineExisted(topicPartitionInfo.getTopic(), pipeline)) {
      LOGGER.info("Topic {} already on uReplicator", topicName);
      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(
          String.format("Failed to add new topic: %s from: %s to: %s, it is already existed!\n",
              topicPartitionInfo.getTopic(), srcCluster, dstCluster));
    } else {
      try {
        _helixMirrorMakerManager
            .addTopicToMirrorMaker(topicPartitionInfo.getTopic(), topicPartitionInfo.getPartition(), srcCluster, dstCluster, pipeline);
        LOGGER.info("Successfully whitelist the topic {} from {} to {}", topicName, srcCluster, dstCluster);
        return new StringRepresentation(
            String.format("Successfully add new topic: %s from: %s to: %s\n",
                topicPartitionInfo.getTopic(), srcCluster, dstCluster));
      } catch (Exception e) {
        LOGGER.info("Failed to whitelist the topic {} from {} to {} due to error {}",
            topicName, srcCluster, dstCluster, e);
        return new StringRepresentation(
            String.format("Failed add new topic: %s from: %s to: %s due to error: %s\n",
                topicPartitionInfo.getTopic(), srcCluster, dstCluster, e.toString()));
      }
    }
  }

  @Override
  @Delete
  public Representation delete() {
    final String topicName = (String) getRequest().getAttributes().get("topicName");
    if (!_helixMirrorMakerManager.isPipelineExisted(topicName)) {
      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(
          String.format("Failed to delete not existed topic: %s\n", topicName));
    }
    try {
      _helixMirrorMakerManager.deleteTopicInMirrorMaker(topicName);
      return new StringRepresentation(
          String.format("Successfully finished delete topic: %s\n", topicName));
    } catch (Exception e) {
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      LOGGER.error("Failed to delete topic: {}, with exception: {}", topicName, e);
      return new StringRepresentation(
          String.format("Failed to delete topic: %s, with exception: %s\n", topicName, e));
    }
  }

}
