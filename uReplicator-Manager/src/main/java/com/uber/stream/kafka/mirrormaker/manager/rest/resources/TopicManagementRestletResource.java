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
package com.uber.stream.kafka.mirrormaker.manager.rest.resources;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import com.uber.stream.kafka.mirrormaker.manager.utils.ControllerUtils;
import com.uber.stream.kafka.mirrormaker.manager.validation.KafkaClusterValidationManager;
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
    KafkaClusterValidationManager kafkaValidationManager = (KafkaClusterValidationManager) getApplication()
        .getContext().getAttributes().get(KafkaClusterValidationManager.class.toString());
    _clusterToObserverMap = kafkaValidationManager.getClusterToObserverMap();
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
        return getResponseJsonStringRepresentation(Status.SUCCESS_OK, "No topic is added in uReplicator!");
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
    if (ControllerUtils.isPipelineName(topicName)) {
      try {
        if (_helixMirrorMakerManager.isPipelineExisted(topicName)) {
          return getResponseJsonStringRepresentation(Status.SUCCESS_OK, getHelixInfoJsonFromManager(topicName));
        } else {
          return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
              String.format("Failed to get view for route: %s, it is not existed!", topicName));
        }
      } catch (Exception e) {
        LOGGER.error("Failed to get view for topic: {} due to exception: {}", topicName, e, e);
        return getResponseJsonStringRepresentation(Status.SERVER_ERROR_INTERNAL,
            String.format("Failed to get view for route: %s due to exception: %s", topicName, e));
      }
    } else if (_helixMirrorMakerManager.isTopicExisted(topicName)) {
      LOGGER.info("_topicToPipelineInstanceMap: {} to {}", topicName, _helixMirrorMakerManager.getTopic(topicName));

      // TODO: add worker information
      JSONObject messageJson = new JSONObject();
      messageJson.put("managerView", getHelixInfoJsonFromManager(topicName));
      messageJson.put("controllerView", _helixMirrorMakerManager.getTopicInfoFromController(topicName));
      return getResponseJsonStringRepresentation(Status.SUCCESS_OK, messageJson);
    } else {
      return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
          String.format("Failed to find topic: %s", topicName));
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
      return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
          String.format("Failed to whitelist topic %s on uReplicator because of not valid pipeline from %s to %s",
              topicName, srcCluster, dstCluster));
    }

    // Check if topic in source cluster
    TopicPartition srcTopicPartitionInfo = _clusterToObserverMap.get(srcCluster)
        .getTopicPartitionWithRefresh(topicName);
    LOGGER.info("Source topicPartitionInfo: {}", srcTopicPartitionInfo);
    if (srcTopicPartitionInfo == null) {
      LOGGER.warn("Failed to whitelist topic {} on uReplicator because of not exists in src cluster", topicName);
      return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
          String.format("Failed to whitelist new topic: %s, it's not existed in source Kafka cluster!", topicName));
    }

    // Check if topic in destination cluster
    TopicPartition dstTopicPartitionInfo = _clusterToObserverMap.get(dstCluster)
        .getTopicPartitionWithRefresh(topicName);
    LOGGER.info("Destination topicPartitionInfo: {}", dstTopicPartitionInfo);
    if (dstTopicPartitionInfo == null) {
      LOGGER.warn("Failed to whitelist topic {} on uReplicator because of not exists in dst cluster", topicName);
      return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
          String.format("Failed to whitelist new topic: %s, it's not existed in destination Kafka cluster!", topicName));
    }

    // TODO: updateCurrentStatus might take a long time
    _helixMirrorMakerManager.updateCurrentStatus();
    String pipeline = ControllerUtils.getPipelineName(srcCluster, dstCluster);
    if (_helixMirrorMakerManager.isTopicPipelineExisted(topicName, pipeline)) {
      LOGGER.info("Topic {} already on uReplicator in pipeline {}", topicName, pipeline);
      return getResponseJsonStringRepresentation(Status.SUCCESS_OK,
          String.format("Failed to add new topic: %s from: %s to: %s, it is already existed!",
              topicName, srcCluster, dstCluster));
    } else {
      try {
        _helixMirrorMakerManager.addTopicToMirrorMaker(srcTopicPartitionInfo.getTopic(),
            srcTopicPartitionInfo.getPartition(), srcCluster, dstCluster, pipeline);
        LOGGER.info("Successfully whitelist the topic {} f rom {} to {}", topicName, srcCluster, dstCluster);
        return getResponseJsonStringRepresentation(Status.SUCCESS_OK,
            String.format("Successfully add new topic: %s from: %s to: %s",
                srcTopicPartitionInfo.getTopic(), srcCluster, dstCluster));
      } catch (Exception e) {
        LOGGER.info("Failed to whitelist the topic {} from {} to {} due to exception {}",
            topicName, srcCluster, dstCluster, e);
        return getResponseJsonStringRepresentation(Status.SERVER_ERROR_INTERNAL,
            String.format("Failed add new topic: %s from: %s to: %s due to exception: %s",
                srcTopicPartitionInfo.getTopic(), srcCluster, dstCluster, e.toString()));
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
    String pipeline = ControllerUtils.getPipelineName(srcCluster, dstCluster);
    if (!_helixMirrorMakerManager.isTopicPipelineExisted(topicName, pipeline)) {
      LOGGER.info("Topic {} doesn't exist in pipeline {}, abandon expanding topic", topicName, pipeline);
      return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
          String.format("Topic %s doesn't exist in pipeline %s, abandon expanding topic!", topicName, pipeline));
    } else {
      try {
        _helixMirrorMakerManager.expandTopicInMirrorMaker(topicName, srcCluster, pipeline,
            Integer.valueOf(newNumPartitions));
        LOGGER.info("Successfully expand the topic {} in pipeline {} to {} partitions",
            topicName, pipeline, newNumPartitions);
        return getResponseJsonStringRepresentation(Status.SUCCESS_OK,
            String.format("Successfully expand the topic %s in pipeline %s to %s partitions",
                topicName, pipeline, newNumPartitions));
      } catch (Exception e) {
        LOGGER.error(String.format("Failed to expand the topic %s in pipeline %s to %s partitions due to exception",
            topicName, pipeline, newNumPartitions) , e);
        return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
            String.format("Failed to expand the topic %s in pipeline %s to %s partitions due to exception: %s",
                topicName, pipeline, newNumPartitions, e.toString()));
      }
    }
  }

  @Override
  @Delete
  public Representation delete() {
    final String topicName = (String) getRequest().getAttributes().get("topicName");

    if (ControllerUtils.isPipelineName(topicName)) {
      // Delete pipeline
      LOGGER.info("Received request to delete pipeline {} on uReplicator ", topicName);

      if (!_helixMirrorMakerManager.isPipelineExisted(topicName)) {
        LOGGER.info("Failed to delete not existed pipeline {}", topicName);
        return getResponseJsonStringRepresentation(Status.CLIENT_ERROR_NOT_FOUND,
            String.format("Failed to delete not existed pipeline: %s", topicName));
      }
      try {
        _helixMirrorMakerManager.deletePipelineInMirrorMaker(topicName);
        LOGGER.info("Successfully delete pipeline: {}", topicName);
        return getResponseJsonStringRepresentation(Status.SUCCESS_OK,
            String.format("Successfully delete pipeline: %s", topicName));
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.error("Failed to delete topic: {} due to exception: {}", topicName, e);
        return getResponseJsonStringRepresentation(Status.SERVER_ERROR_INTERNAL,
            String.format("Failed to delete topic: %s due to exception: %s", topicName, e));
      }
    } else {
      // Delete topic
      Form queryParams = getRequest().getResourceRef().getQueryAsForm();
      String srcCluster = queryParams.getFirstValue("src");
      String dstCluster = queryParams.getFirstValue("dst");
      String pipeline = ControllerUtils.getPipelineName(srcCluster, dstCluster);

      LOGGER.info("Received request to delete topic {} from {} to {} on uReplicator ",
          topicName, srcCluster, dstCluster);

      if (_helixMirrorMakerManager.isTopicPipelineExisted(topicName, pipeline)) {
        try {
          _helixMirrorMakerManager.deleteTopicInMirrorMaker(topicName, srcCluster, dstCluster, pipeline);
          LOGGER.info("Successfully delete topic: {} from {} to {}", topicName, srcCluster, dstCluster);
          return getResponseJsonStringRepresentation(Status.SUCCESS_OK,
              String.format("Successfully delete topic: %s from %s to %s", topicName, srcCluster, dstCluster));
        } catch (Exception e) {
          LOGGER.info("Failed to delete the topic {} from {} to {} due to exception {}",
              topicName, srcCluster, dstCluster, e);
          return getResponseJsonStringRepresentation(Status.SERVER_ERROR_INTERNAL,
              String.format("Failed to delete topic: %s from: %s to: %s due to exception: %s",
                  topicName, srcCluster, dstCluster, e.toString()));
        }
      } else {
        LOGGER.info("Failed to delete not existed topic {} in pipeline {}", topicName, pipeline);
        // return 200 for deleting non-exist topic so caller won't try to delete again
        return getResponseJsonStringRepresentation(Status.SUCCESS_OK,
            String.format("Failed to delete not existed topic: %s in pipeline: %s", topicName, pipeline));
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

    helixInfoJson.put("serverToPartitionMapping", serverToPartitionMappingJson);
    helixInfoJson.put("serverToNumPartitionsMapping", serverToNumPartitionsMappingJson);

    if (ControllerUtils.isPipelineName(topicName)) {
      return helixInfoJson;
    }

    JSONObject workerMappingJson = new JSONObject();
    for (InstanceTopicPartitionHolder itph : _helixMirrorMakerManager.getTopicToPipelineInstanceMap()
        .get(topicName).values()) {
      workerMappingJson.put(itph.getRouteString(), itph.getWorkerSet());
    }
    helixInfoJson.put("workers", workerMappingJson);

    return helixInfoJson;
  }

  private boolean isValidPipeline(String src, String dst) {
    return _conf.getSourceClusters().contains(src) && _conf.getDestinationClusters().contains(dst);
  }

  private StringRepresentation getResponseJsonStringRepresentation(Status status, Object message) {
    JSONObject responseJson = new JSONObject();
    responseJson.put("status", status.getCode());
    responseJson.put("message", message);
    getResponse().setStatus(status);
    return new StringRepresentation(responseJson.toJSONString());
  }

}
