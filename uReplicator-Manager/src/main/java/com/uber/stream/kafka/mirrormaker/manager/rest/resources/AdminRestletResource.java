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

import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import com.uber.stream.kafka.mirrormaker.manager.core.WorkerHelixManager;
import org.apache.commons.lang3.StringUtils;
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
import com.google.common.base.Strings;
import com.uber.stream.kafka.mirrormaker.manager.core.AdminHelper;

/**
 * Rest API for topic management
 */
public class AdminRestletResource extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdminRestletResource.class);

  private static final boolean ENABLE_PER_ROUTE_CHANGE = false;

  private final ControllerHelixManager _helixMirrorMakerManager;

  public AdminRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);

    _helixMirrorMakerManager = (ControllerHelixManager) getApplication().getContext()
        .getAttributes().get(ControllerHelixManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    final String opt = (String) getRequest().getAttributes().get("opt");
    JSONObject responseJson = new JSONObject();
    if ("autoscaling_status".equalsIgnoreCase(opt)) {
      responseJson.put("auto_scaling", _helixMirrorMakerManager.isAutoScalingEnabled());
    } else if ("autobalancing_status".equalsIgnoreCase(opt)) {
      responseJson.put("auto_balancing", _helixMirrorMakerManager.isAutoBalancingEnabled());
    } else if ("controller_autobalancing".equalsIgnoreCase(opt)) {
      AdminHelper helper = new AdminHelper(_helixMirrorMakerManager);
      return new StringRepresentation(helper.getControllerAutobalancingStatus(null, null)
          .toJSONString());
    } else if ("worker_number_override".equalsIgnoreCase(opt)) {
      responseJson.put("worker_number_override", _helixMirrorMakerManager.getRouteWorkerOverride());
    } else {
      LOGGER.info("No valid input!");
      responseJson.put("opt", "No valid input!");
    }
    return new StringRepresentation(responseJson.toJSONString());
  }

  @Post
  public Representation post(Representation entity) {
    // TODO: separate manager and controller operation
    final String opt = (String) getRequest().getAttributes().get("opt");
    JSONObject responseJson = new JSONObject();
    if ("disable_autoscaling".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoScaling();
      LOGGER.info("Disabled autoscaling!");
      responseJson.put("opt", "disable_autoscaling");
      responseJson.put("auto_scaling", _helixMirrorMakerManager.isAutoScalingEnabled());
    } else if ("enable_autoscaling".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoScaling();
      LOGGER.info("Enabled autobalancing!");
      responseJson.put("opt", "enable_autobalancing");
      responseJson.put("auto_scaling", _helixMirrorMakerManager.isAutoScalingEnabled());
    } else if ("disable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoBalancing();
      LOGGER.info("Disabled autobalancing!");
      responseJson.put("opt", "disable_autobalancing");
      responseJson.put("auto_balancing", _helixMirrorMakerManager.isAutoBalancingEnabled());
    } else if ("enable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoBalancing();
      LOGGER.info("Enabled autobalancing!");
      responseJson.put("opt", "enableAutoBalancing");
      responseJson.put("auto_balancing", _helixMirrorMakerManager.isAutoBalancingEnabled());
    } else if ("controller_autobalancing".equalsIgnoreCase(opt)) {
      Form queryParams = getRequest().getResourceRef().getQueryAsForm();
      String srcCluster = ENABLE_PER_ROUTE_CHANGE ? queryParams.getFirstValue("srcCluster", true) : "";
      String dstCluster = ENABLE_PER_ROUTE_CHANGE ? queryParams.getFirstValue("dstCluster", true) : "";
      String enabledStr = queryParams.getFirstValue("enabled", true);
      if (Strings.isNullOrEmpty(enabledStr) || (Strings.isNullOrEmpty(srcCluster) != Strings.isNullOrEmpty(dstCluster))) {
        getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        responseJson.put("status", Status.CLIENT_ERROR_BAD_REQUEST.getCode());
        responseJson.put("message", String.format("invalid operation"));
        return new StringRepresentation(responseJson.toJSONString());
      } else {
        return new StringRepresentation(setControllerAutobalancing(srcCluster, dstCluster, enabledStr).toJSONString());
      }
    } else if ("force_rebalance".equalsIgnoreCase(opt) || "manual_rebalance".equalsIgnoreCase(opt)) {
      try {
        boolean force = "force_rebalance".equalsIgnoreCase(opt);
        _helixMirrorMakerManager.handleLiveInstanceChange(false, force);
        responseJson.put("status", Status.SUCCESS_OK.getCode());
        return new StringRepresentation(responseJson.toJSONString());
      } catch (Exception e) {
        LOGGER.error("manual re-balance failed due to exception: {}", e, e);
        responseJson.put("status", Status.SERVER_ERROR_INTERNAL.getCode());
        responseJson
            .put("message", String.format("manual re-balance failed due to exception: %s", e));

        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      }
    } else if ("worker_number_override".equalsIgnoreCase(opt)) {
      Form queryParams = getRequest().getResourceRef().getQueryAsForm();

      String routeString = queryParams.getFirstValue("route", true);
      String workerNum = queryParams.getFirstValue("workerNumber", true);
      Integer workerCount = Integer.parseInt(workerNum);
      boolean validRoute = false;
      String example = "";
      for (String pipeline : _helixMirrorMakerManager.getPipelineToInstanceMap().keySet()) {
        for (InstanceTopicPartitionHolder itph : _helixMirrorMakerManager.getPipelineToInstanceMap().get(pipeline)) {
          if (itph.getRouteString().equalsIgnoreCase(routeString)) {
            validRoute = true;
          }
          if (StringUtils.isEmpty(example)) {
            example = itph.getRouteString();
          }
        }
      }
      if (validRoute) {
        _helixMirrorMakerManager.updateRouteWorkerOverride(routeString, workerCount);
        responseJson.put("worker_number_override", _helixMirrorMakerManager.getRouteWorkerOverride());
      } else {
        return new StringRepresentation("invalid route string, route sample: " + example);
      }
    } else {
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      responseJson.put("status", Status.CLIENT_ERROR_BAD_REQUEST.getCode());
      responseJson.put("message", String.format("invalid operation"));
    }
    return new StringRepresentation(responseJson.toJSONString());
  }

  @Delete
  public Representation delete() {
    final String opt = (String) getRequest().getAttributes().get("opt");
    JSONObject responseJson = new JSONObject();
    if ("worker_number_override".equalsIgnoreCase(opt)) {
      Form queryParams = getRequest().getResourceRef().getQueryAsForm();
      String routeString = queryParams.getFirstValue("route", true);
      if (StringUtils.isEmpty(routeString)) {
        getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        responseJson.put("status", Status.CLIENT_ERROR_BAD_REQUEST.getCode());
        responseJson.put("message", String.format("missing parameter route"));
      } else {
        if (_helixMirrorMakerManager.getRouteWorkerOverride().containsKey(routeString)) {
          _helixMirrorMakerManager.updateRouteWorkerOverride(routeString, -1);
          responseJson.put("worker_number_override", _helixMirrorMakerManager.getRouteWorkerOverride());
          responseJson.put("status", Status.SUCCESS_OK);
        } else {
          responseJson.put("message", String.format("route override not exists"));
          responseJson.put("status", Status.SUCCESS_OK);
        }
      }
    } else {
      LOGGER.info("No valid input!");
      responseJson.put("opt", "No valid input!");
    }
    return new StringRepresentation(responseJson.toJSONString());
  }

  private JSONObject setControllerAutobalancing(String srcCluster, String dstCluster, String enabledStr) {
    JSONObject responseJson = new JSONObject();
    boolean enabled = Boolean.parseBoolean(enabledStr);
    AdminHelper helper = new AdminHelper(_helixMirrorMakerManager);
    Map<String, Boolean> status = helper.setControllerAutobalancing(srcCluster, dstCluster, enabled);
    JSONObject statusJson = new JSONObject();
    for (String instance : status.keySet()) {
      statusJson.put(instance, status.get(instance) ? "success" : "failed");
    }
    JSONObject queriedResult = helper.getControllerAutobalancingStatus(srcCluster, dstCluster);
    responseJson.put("execution", statusJson);
    responseJson.put("status", queriedResult);
    if (Strings.isNullOrEmpty(srcCluster) && Strings.isNullOrEmpty(dstCluster)) {
      if (enabled) {
        _helixMirrorMakerManager.enableAutoScaling();
      } else {
        _helixMirrorMakerManager.disableAutoScaling();
      }
    }
    responseJson.put("managerAutoscaling", _helixMirrorMakerManager.isAutoScalingEnabled());
    return responseJson;
  }
}
