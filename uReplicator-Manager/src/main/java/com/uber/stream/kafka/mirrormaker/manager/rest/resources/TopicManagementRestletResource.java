package com.uber.stream.kafka.mirrormaker.manager.rest.resources;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import com.uber.stream.kafka.mirrormaker.manager.validation.SourceKafkaClusterValidationManager;
import java.util.ArrayList;
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
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest API for topic management
 */
public class TopicManagementRestletResource extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicManagementRestletResource.class);

  private static final String SEPARATOR = "@";

  private final ManagerConf _conf;
  private final ControllerHelixManager _helixMirrorMakerManager;
  private final Map<String, KafkaBrokerTopicObserver> _clusterToObserverMap;

  public TopicManagementRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);

    _conf = (ManagerConf) getApplication().getContext().getAttributes().get(ManagerConf.class.toString());
    _helixMirrorMakerManager = (ControllerHelixManager) getApplication().getContext()
        .getAttributes().get(ControllerHelixManager.class.toString());
    SourceKafkaClusterValidationManager srcKafkaValidationManager = (SourceKafkaClusterValidationManager) getApplication()
        .getContext().getAttributes().get(SourceKafkaClusterValidationManager.class.toString());
    _clusterToObserverMap = srcKafkaValidationManager.getClusterToObserverMap();
  }

  @Override
  @Get
  public Representation get() {
    final String topicName = (String) getRequest().getAttributes().get("topicName");

    // Get whole picture of the deployment
    if (topicName == null) {
      // TODO: updateCurrentStatus might take a long time
      _helixMirrorMakerManager.updateCurrentStatus();
      Map<String, Map<String, InstanceTopicPartitionHolder>> topicToPipelineInstanceMap = _helixMirrorMakerManager
          .getTopicToPipelineInstanceMap();

      if (topicToPipelineInstanceMap == null || topicToPipelineInstanceMap.isEmpty()) {
        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
        responseJson.put("message", "No topic is added in uReplicator!");

        return new StringRepresentation(responseJson.toJSONString());
      } else {
        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SUCCESS_OK.getCode());

        JSONObject topicToInstanceMappingJson = new JSONObject();
        topicToInstanceMappingJson.put("topics", _helixMirrorMakerManager.getTopicToPipelineInstanceMap().keySet());
        for (String topic : topicToPipelineInstanceMap.keySet()) {
          JSONObject topicInfoJson = new JSONObject();

          for (String pipeline : topicToPipelineInstanceMap.get(topic).keySet()) {
            JSONObject instanceInfoJson = new JSONObject();
            instanceInfoJson.put("controller", topicToPipelineInstanceMap.get(topic).get(pipeline).getInstanceName());
            instanceInfoJson.put("route", topicToPipelineInstanceMap.get(topic).get(pipeline).getRouteString());
            instanceInfoJson.put("workers", topicToPipelineInstanceMap.get(topic).get(pipeline).getWorkerSet());
            topicInfoJson.put(pipeline, instanceInfoJson);
          }
          topicToInstanceMappingJson.put(topic, topicInfoJson);
        }
        responseJson.put("message", topicToInstanceMappingJson);

        return new StringRepresentation(responseJson.toJSONString());
      }
    }
    // Get pipeline information
    if (topicName.startsWith(SEPARATOR)) {
      try {
        if (_helixMirrorMakerManager.isPipelineExisted(topicName)) {
          // TODO: add worker information
          JSONObject responseJson = new JSONObject();
          responseJson.put("status", Status.SUCCESS_OK.getCode());
          responseJson.put("message", getHelixInfoJsonFromManager(topicName));

          return new StringRepresentation(responseJson.toJSONString());
        } else {
          JSONObject responseJson = new JSONObject();
          responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
          responseJson.put("message",
              String.format("Failed to get view for topic: %s, it is not existed!", topicName));

          getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
          return new StringRepresentation(responseJson.toJSONString());
        }
      } catch (Exception e) {
        LOGGER.error("Failed to get view for topic: {} due to exception: {}", topicName, e);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SERVER_ERROR_INTERNAL.getCode());
        responseJson.put("message",
            String.format("Failed to get view for topic: %s due to exception: %s", topicName, e));

        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(responseJson.toJSONString());
      }
    } else if (_helixMirrorMakerManager.isTopicExisted(topicName)) {
      LOGGER.info("_topicToPipelineInstanceMap: {} to {}", topicName, _helixMirrorMakerManager.getTopic(topicName));

      // TODO: add worker information
      JSONObject responseJson = new JSONObject();
      JSONObject messageJson = new JSONObject();
      messageJson.put("managerView", getHelixInfoJsonFromManager(topicName));
      messageJson.put("controllerView", _helixMirrorMakerManager.getTopicInfoFromController(topicName));
      responseJson.put("status", Status.SUCCESS_OK.getCode());
      responseJson.put("message", messageJson);

      return new StringRepresentation(responseJson.toJSONString());
    } else {
      JSONObject responseJson = new JSONObject();
      responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
      responseJson.put("message", String.format("Failed to find topic: %s", topicName));

      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(responseJson.toJSONString());
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

    // TODO: validate src->dst combination
    if (!isValidPipeline(srcCluster, dstCluster)) {
      LOGGER.warn("Failed to whitelist topic {} on uReplicator because of not valid pipeline from {} to {}",
          topicName, srcCluster, dstCluster);

      JSONObject responseJson = new JSONObject();
      responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
      responseJson.put("message",
          String.format("Failed to whitelist topic %s on uReplicator because of not valid pipeline from %s to %s",
              topicName, srcCluster, dstCluster));

      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(responseJson.toJSONString());
    }

    TopicPartition topicPartitionInfo = _clusterToObserverMap.get(srcCluster).getTopicPartitionWithRefresh(topicName);
    LOGGER.info("topicPartitionInfo: {}", topicPartitionInfo);
    if (topicPartitionInfo == null) {
      LOGGER.warn("Failed to whitelist topic {} on uReplicator because of not exists in src cluster", topicName);

      JSONObject responseJson = new JSONObject();
      responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
      responseJson.put("message",
          String.format("Failed to whitelist new topic: %s, it's not existed in source Kafka cluster!", topicName));

      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(responseJson.toJSONString());
    }

    // TODO: updateCurrentStatus might take a long time
    _helixMirrorMakerManager.updateCurrentStatus();
    String pipeline = SEPARATOR + srcCluster + SEPARATOR + dstCluster;
    if (_helixMirrorMakerManager.isTopicPipelineExisted(topicName, pipeline)) {
      LOGGER.info("Topic {} already on uReplicator in pipeline {}", topicName, pipeline);

      JSONObject responseJson = new JSONObject();
      responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
      responseJson.put("message",
          String.format("Failed to add new topic: %s from: %s to: %s, it is already existed!",
              topicName, srcCluster, dstCluster));

      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(responseJson.toJSONString());
    } else {
      try {
        _helixMirrorMakerManager.addTopicToMirrorMaker(topicPartitionInfo.getTopic(), topicPartitionInfo.getPartition(),
            srcCluster, dstCluster, pipeline);
        LOGGER.info("Successfully whitelist the topic {} from {} to {}", topicName, srcCluster, dstCluster);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SUCCESS_OK.getCode());
        responseJson.put("message",
            String.format("Successfully add new topic: %s from: %s to: %s",
                topicPartitionInfo.getTopic(), srcCluster, dstCluster));

        return new StringRepresentation(responseJson.toJSONString());
      } catch (Exception e) {
        LOGGER.info("Failed to whitelist the topic {} from {} to {} due to exception {}",
            topicName, srcCluster, dstCluster, e);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SERVER_ERROR_INTERNAL.getCode());
        responseJson.put("message",
            String.format("Failed add new topic: %s from: %s to: %s due to exception: %s",
                topicPartitionInfo.getTopic(), srcCluster, dstCluster, e.toString()));

        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(responseJson.toJSONString());
      }
    }
  }

  @Override
  @Put
  public Representation put(Representation entity) {
    final String topicName = (String) getRequest().getAttributes().get("topicName");
    Form queryParams = getRequest().getResourceRef().getQueryAsForm();
    String srcCluster = queryParams.getFirstValue("src");
    String dstCluster = queryParams.getFirstValue("dst");
    String newNumPartitions = queryParams.getFirstValue("partitions");

    LOGGER.info("Received request to expand topic {} from {} to {} to {} partitions on uReplicator",
        topicName, srcCluster, dstCluster, newNumPartitions);

    _helixMirrorMakerManager.updateCurrentStatus();
    String pipeline = SEPARATOR + srcCluster + SEPARATOR + dstCluster;
    if (!_helixMirrorMakerManager.isTopicPipelineExisted(topicName, pipeline)) {
      LOGGER.info("Topic {} doesn't exist in pipeline {}, abandon expanding topic", topicName, pipeline);
      JSONObject responseJson = new JSONObject();
      responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
      responseJson.put("message",
          String.format("Topic %s doesn't exist in pipeline %s, abandon expanding topic!", topicName, pipeline));

      getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation(responseJson.toJSONString());
    } else {
      try {
        _helixMirrorMakerManager.expandTopicInMirrorMaker(topicName, srcCluster, pipeline,
            Integer.valueOf(newNumPartitions));
        LOGGER.info("Successfully expand the topic {} in pipeline {} to {} partitions",
            topicName, pipeline, newNumPartitions);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SUCCESS_OK.getCode());
        responseJson.put("message",
            String.format("Successfully expand the topic %s in pipeline %s to %s partitions",
                topicName, pipeline, newNumPartitions));

        return new StringRepresentation(responseJson.toJSONString());
      } catch (Exception e) {
        LOGGER.info("Failed to expand the topic {} in pipeline {} to {} partitions due to exception {}",
            topicName, pipeline, newNumPartitions, e);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SERVER_ERROR_INTERNAL.getCode());
        responseJson.put("message",
            String.format("Failed to expand the topic %s in pipeline %s to %s partitions due to exception: %s",
                topicName, pipeline, newNumPartitions, e.toString()));

        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(responseJson.toJSONString());
      }
    }
  }

  @Override
  @Delete
  public Representation delete() {
    final String topicName = (String) getRequest().getAttributes().get("topicName");

    if (topicName.startsWith(SEPARATOR)) {
      // Delete pipeline
      LOGGER.info("Received request to delete pipeline {} on uReplicator ", topicName);

      if (!_helixMirrorMakerManager.isPipelineExisted(topicName)) {
        LOGGER.info("Failed to delete not existed pipeline {}", topicName);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
        responseJson.put("message",
            String.format("Failed to delete not existed pipeline: %s", topicName));

        getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation(responseJson.toJSONString());
      }
      try {
        _helixMirrorMakerManager.deletePipelineInMirrorMaker(topicName);

        LOGGER.info("Successfully delete pipeline: {}", topicName);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SUCCESS_OK.getCode());
        responseJson.put("message", String.format("Successfully delete pipeline: %s", topicName));

        return new StringRepresentation(responseJson.toJSONString());
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.error("Failed to delete topic: {} due to exception: {}", topicName, e);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.SERVER_ERROR_INTERNAL.getCode());
        responseJson.put("message",
            String.format("Failed to delete topic: %s due to exception: %s", topicName, e));

        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(responseJson.toJSONString());
      }
    } else {
      // Delete topic
      Form queryParams = getRequest().getResourceRef().getQueryAsForm();
      String srcCluster = queryParams.getFirstValue("src");
      String dstCluster = queryParams.getFirstValue("dst");
      String pipeline = SEPARATOR + srcCluster + SEPARATOR + dstCluster;

      LOGGER.info("Received request to delete topic {} from {} to {} on uReplicator ",
          topicName, srcCluster, dstCluster);

      if (_helixMirrorMakerManager.isTopicPipelineExisted(topicName, pipeline)) {
        try {
          _helixMirrorMakerManager.deleteTopicInMirrorMaker(topicName, srcCluster, dstCluster, pipeline);

          LOGGER.info("Successfully delete topic: {} from {} to {}", topicName, srcCluster, dstCluster);

          JSONObject responseJson = new JSONObject();
          responseJson.put("status", Status.SUCCESS_OK.getCode());
          responseJson.put("message", String.format("Successfully delete topic: %s from %s to %s",
              topicName, srcCluster, dstCluster));

          return new StringRepresentation(responseJson.toJSONString());
        } catch (Exception e) {
          LOGGER.info("Failed to delete the topic {} from {} to {} due to exception {}",
              topicName, srcCluster, dstCluster, e);

          JSONObject responseJson = new JSONObject();
          responseJson.put("status", Status.SERVER_ERROR_INTERNAL.getCode());
          responseJson.put("message",
              String.format("Failed to delete new topic: %s from: %s to: %s due to exception: %s",
                  topicName, srcCluster, dstCluster, e.toString()));

          getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
          return new StringRepresentation(responseJson.toJSONString());
        }
      } else {
        LOGGER.info("Failed to delete not existed topic {} in pipeline {}", topicName, pipeline);

        JSONObject responseJson = new JSONObject();
        responseJson.put("status", Status.CLIENT_ERROR_NOT_FOUND.getCode());
        responseJson.put("message",
            String.format("Failed to delete not existed topic: %s in pipeline: %s", topicName, pipeline));

        getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        return new StringRepresentation(responseJson.toJSONString());
      }
    }
  }

  private JSONObject getHelixInfoJsonFromManager(String topicName) {
    IdealState idealStateForTopic = _helixMirrorMakerManager.getIdealStateForTopic(topicName);
    ExternalView externalViewForTopic = _helixMirrorMakerManager.getExternalViewForTopic(topicName);
    JSONObject helixInfoJson = new JSONObject();
    helixInfoJson.put("topic", topicName);
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
    helixInfoJson.put("externalView", externalViewPartitionToServerMappingJson);

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
    helixInfoJson.put("idealState", idealStatePartitionToServerMappingJson);
    Map<String, List<String>> serverToPartitionMapping = new HashMap<>();
    JSONObject serverToPartitionMappingJson = new JSONObject();
    JSONObject serverToNumPartitionsMappingJson = new JSONObject();

    if (externalViewForTopic != null) {
      for (String partition : externalViewForTopic.getPartitionSet()) {
        Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
        for (String server : stateMap.keySet()) {
          if (stateMap.get(server).equals("ONLINE")) {
            if (!serverToPartitionMapping.containsKey(server)) {
              serverToPartitionMapping.put(server, new ArrayList<>());
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

    JSONObject workerMappingJson = new JSONObject();
    for (InstanceTopicPartitionHolder itph : _helixMirrorMakerManager.getTopicToPipelineInstanceMap()
        .get(topicName).values()) {
      workerMappingJson.put(itph.getRouteString(), itph.getWorkerSet());
    }
    helixInfoJson.put("workers", workerMappingJson);

    helixInfoJson.put("serverToPartitionMapping", serverToPartitionMappingJson);
    helixInfoJson.put("serverToNumPartitionsMapping", serverToNumPartitionsMappingJson);

    return helixInfoJson;
  }

  private boolean isValidPipeline(String src, String dst) {
    return _conf.getSourceClusters().contains(src) && _conf.getDestinationClusters().contains(dst);
  }

}
