package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.core.WorkloadInfoRetriever;
import com.uber.stream.kafka.mirrormaker.controller.core.AutoRebalanceLiveInstanceChangeListener;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;

import java.util.Iterator;
import java.util.PriorityQueue;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorMakerManagerRestletResource extends ServerResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(MirrorMakerManagerRestletResource.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public MirrorMakerManagerRestletResource() {
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
      WorkloadInfoRetriever workloadRetriever = _helixMirrorMakerManager.getWorkloadInfoRetriever();
      Iterator<InstanceTopicPartitionHolder> iter = currentServingInstance.iterator();
      JSONObject instanceMapJson = new JSONObject();
      while (iter.hasNext()) {
        InstanceTopicPartitionHolder instance = iter.next();
        String name = instance.getInstanceName();
        if (instanceName == null || instanceName.equals(name)) {
          if (!instanceMapJson.containsKey(name)) {
            instanceMapJson.put(name, new JSONArray());
          }
          double totalWorkload = 0;
          for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
            double tpw = workloadRetriever.topicWorkload(tp.getTopic()).getBytesPerSecondPerPartition();
            totalWorkload += tpw;
            instanceMapJson.getJSONArray(name).add(tp.getTopic() + "." + tp.getPartition() + ":" + Math.round(tpw));
          }
          instanceMapJson.getJSONArray(name).add("TOTALWORKLOAD." + instance.getServingTopicPartitionSet().size()
              + ":" + Math.round(totalWorkload));
        }
      }
      responseJson.put("instances", instanceMapJson);

      JSONArray blacklistedArray = new JSONArray();
      blacklistedArray.addAll(_helixMirrorMakerManager.getBlacklistedInstances());
      responseJson.put("blacklisted", blacklistedArray);

      JSONArray allInstances = new JSONArray();
      allInstances.addAll(_helixMirrorMakerManager.getCurrentLiveInstanceNames());
      responseJson.put("allInstances", allInstances);

      return new StringRepresentation(responseJson.toJSONString());
    } catch (Exception e) {
      LOGGER.error("Got error during processing Get request", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(String
          .format("Failed to get serving topics for %s, with exception: %s",
              instanceName == null ? "all instances" : instanceName, e));
    }
  }

  @Override
  @Post
  public Representation post(Representation entity) {
    final String instanceName = (String) getRequest().getAttributes().get("instanceName");
    if (instanceName != null) {
      // whitelist a worker
      try {
        _helixMirrorMakerManager.whitelistInstance(instanceName);
        return new StringRepresentation(String.format("Instance %s is whitelisted", instanceName));
      } catch (Exception e) {
        LOGGER.error("Got error during processing Post request", e);
        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(String
            .format("Failed to whitelist instance %s, with exception: %s", instanceName, e));
      }
    } else {
      // rebalance the whole cluster
      try {
        AutoRebalanceLiveInstanceChangeListener rebalancer = _helixMirrorMakerManager.getRebalancer();
        if (rebalancer.triggerRebalanceCluster()) {
          return new StringRepresentation("Cluster is rebalanced\n");
        }
        return new StringRepresentation("Skipped rebalancing cluster\n");
      } catch (Exception e) {
        LOGGER.error("Got error during processing POST request", e);
        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(String.format("Failed to rebalance cluster with exception: %s", e));
      }
    }
  }

  @Override
  @Delete
  public Representation delete() {
    final String instanceName = (String) getRequest().getAttributes().get("instanceName");
    if (instanceName == null) {
      return new StringRepresentation("Instance name is required to blacklist");
    }
    // blacklist a worker
    try {
      _helixMirrorMakerManager.blacklistInstance(instanceName);
      return new StringRepresentation(String.format("Instance %s is blacklisted", instanceName));
    } catch (Exception e) {
      LOGGER.error("Got error during processing Delete request", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(String
          .format("Failed to blacklist instance %s, with exception: %s", instanceName, e));
    }
  }

}
