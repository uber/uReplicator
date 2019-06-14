/*
 * Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.ureplicator.worker;

import org.apache.commons.lang.StringUtils;
import org.restlet.data.Form;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest Resource to override rate limiter
 */
public class RateLimiterResource extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(RateLimiterResource.class);
  private final WorkerInstance workerInstance;

  public RateLimiterResource() {
    this.workerInstance = (WorkerInstance) getApplication().getContext()
        .getAttributes().get(WorkerInstance.class.toString());
  }

  @Override
  @Put
  public Representation put(Representation entity) {
    Form params = getRequest().getResourceRef().getQueryAsForm();
    String rate = params.getFirstValue("messagerate");
    if (StringUtils.isEmpty(rate)) {
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(
          String.format("Failed to add new rate limit due to missing parameter messagerate\n"));
    }
    try {
      workerInstance.setMessageRatePerSecond(Double.parseDouble(rate));
      LOGGER.info("Set messageRate to {} finished", rate);
    } catch (Exception e) {
      getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      LOGGER.error("Set messageRate to {} failed", rate, e);
      return new StringRepresentation(
          String.format("Failed to add new topic, with exception: %s\n", e));
    }
    return new StringRepresentation(
        String.format("Successfully set rate: %s\n", rate));
  }
}
