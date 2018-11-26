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

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest API for topic management
 */
public class AdminRestletResource extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdminRestletResource.class);

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
    if ("disable_autoscaling".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoScaling();
      LOGGER.info("Disabled autoscaling!");
      return new StringRepresentation("Disabled autoscaling!\n");
    } else if ("enable_autoscaling".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoScaling();
      LOGGER.info("Enabled autobalancing!");
      return new StringRepresentation("Enabled autoscaling!\n");
    } else if ("autoscaling_status".equalsIgnoreCase(opt)) {
      if (_helixMirrorMakerManager.isAutoScalingEnabled()) {
        return new StringRepresentation("enabled\n");
      } else {
        return new StringRepresentation("disabled\n");
      }
    } else if ("disable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoBalancing();
      LOGGER.info("Disabled autobalancing!");
      return new StringRepresentation("Disabled autobalancing!\n");
    } else if ("enable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoBalancing();
      LOGGER.info("Enabled autobalancing!");
      return new StringRepresentation("Enabled autobalancing!\n");
    } else if ("autobalancing_status".equalsIgnoreCase(opt)) {
      if (_helixMirrorMakerManager.isAutoBalancingEnabled()) {
        return new StringRepresentation("enabled\n");
      } else {
        return new StringRepresentation("disabled\n");
      }
    }
    LOGGER.info("No valid input!");
    return new StringRepresentation("No valid input!\n");
  }


  @Post
  public Representation post(Representation entity) {
    Form queryParams = getRequest().getResourceRef().getQueryAsForm();
    String forceRebalanceStr = queryParams.getFirstValue("forceRebalance", true);
    Boolean forceRebalance = Boolean.parseBoolean(forceRebalanceStr);
    JSONObject responseJson = new JSONObject();

    if (forceRebalance) {
      try {
        _helixMirrorMakerManager.handleLiveInstanceChange(false, true);
        responseJson.put("status", Status.SUCCESS_OK.getCode());

        return new StringRepresentation(responseJson.toJSONString());
      } catch (Exception e) {
        LOGGER.error("manual re-balance failed due to exception: {}", e, e);

        responseJson.put("status", Status.SERVER_ERROR_INTERNAL.getCode());
        responseJson.put("message",
            String.format("manual re-balance failed due to exception: {}", e));

        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(responseJson.toJSONString());
      }
    } else {
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      responseJson.put("status", Status.CLIENT_ERROR_BAD_REQUEST.getCode());
      responseJson.put("message",
          String.format("invalid operation"));
      return new StringRepresentation(responseJson.toJSONString());
    }
  }
}
