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
package com.uber.stream.kafka.mirrormaker.controller.rest;

import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerRequestURLBuilder;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NoProgressTopicPartitionRestletResourceTest extends RestTestBase {
  @Test
  public void testGet() {
    // Get call
    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getNoProgressRequestUrl();
    Response response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(), "{}");
    System.out.println(response.getEntityAsText());
  }
}
