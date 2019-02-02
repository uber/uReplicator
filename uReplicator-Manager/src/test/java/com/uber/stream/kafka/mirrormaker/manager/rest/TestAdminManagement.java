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
package com.uber.stream.kafka.mirrormaker.manager.rest;

import com.uber.stream.kafka.mirrormaker.manager.utils.ManagerRequestURLBuilder;

import org.testng.annotations.Test;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Status;
import org.testng.Assert;

public class TestAdminManagement extends RestTestBase {
  @Test
  public void testPostRebalance() {
    Request request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL).postInstanceRebalance();
    Response response = HTTP_CLIENT.handle(request);
    String responseString = response.getEntityAsText();
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(responseString, "{\"status\":200}");
  }

  @Test
  public void testPostControllerRebalance() {
    Request request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL).postSetControllerRebalance(true);
    Response response = HTTP_CLIENT.handle(request);
    String responseString = response.getEntityAsText();
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(responseString, "{\"execution\":{},\"managerAutoscaling\":true,\"status\":{}}");
    request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL).postSetControllerRebalance("sjc1a", "sjc1-agg1", true);
    response = HTTP_CLIENT.handle(request);
    responseString = response.getEntityAsText();
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(responseString, "{\"execution\":{},\"managerAutoscaling\":true,\"status\":{}}");
  }
  @Test
  public void testGetControllerRebalance() {
    Request request = ManagerRequestURLBuilder.baseUrl(REQUEST_URL).getControllerRebalanceStatus();
    Response response = HTTP_CLIENT.handle(request);
    String responseString = response.getEntityAsText();
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(responseString, "{}");
  }

}
