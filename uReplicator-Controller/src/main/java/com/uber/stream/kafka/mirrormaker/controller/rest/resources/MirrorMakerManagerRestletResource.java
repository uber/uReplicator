package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.controller.core.TopicPartition;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorMakerManagerRestletResource extends ServerResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MirrorMakerManagerRestletResource.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public MirrorMakerManagerRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    final String instanceName = (String) getRequest().getAttributes().get("instanceName");
    try {
      JSONObject responseJson = new JSONObject();

      PriorityQueue<InstanceTopicPartitionHolder> currentServingInstance = _helixMirrorMakerManager
          .getCurrentServingInstance();
      Iterator<InstanceTopicPartitionHolder> iter = currentServingInstance.iterator();
      JSONObject instanceMapJson = new JSONObject();
      while (iter.hasNext()) {
        InstanceTopicPartitionHolder instance = iter.next();
        String name = instance.getInstanceName();
        if (instanceName == null || instanceName.equals(name)) {
          for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
            if (!instanceMapJson.containsKey(name)) {
              instanceMapJson.put(name, new JSONArray());
            }
            instanceMapJson.getJSONArray(name).add(tp.getTopic() + ":" + tp.getPartition());
          }
        }
      }
      responseJson.put("instances", instanceMapJson);
      return new StringRepresentation(responseJson.toJSONString());
    } catch (Exception e) {
      LOGGER.error("Got error during processing Get request", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(String
          .format("Failed to get serving topics for %s, with exception: %s",
              instanceName == null ? "all instances" : instanceName, e));
    }
  }

}
