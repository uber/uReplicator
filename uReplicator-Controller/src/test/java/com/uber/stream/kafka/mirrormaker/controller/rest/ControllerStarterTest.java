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
package com.uber.stream.kafka.mirrormaker.controller.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerRequestURLBuilder;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Status;
import org.testng.Assert;
import org.testng.annotations.*;

public class ControllerStarterTest extends RestTestBase {
  @Test
  public void testGet() {
    // Get call
    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("");
    Response response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(), "No topic is added in MirrorMaker Controller!");
    System.out.println(response.getEntityAsText());

    // Create topic
    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicCreationRequestUrl("testTopic0", 8);
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully add new topic: {topic: testTopic0, partition: 8, pipeline: null}");
    Assert.assertTrue(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic0"));

    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(), "Current serving topics: [testTopic0]");
    System.out.println(response.getEntityAsText());
    try {
      Thread.sleep(4000);
    } catch (InterruptedException e) {
    }

    // Get call
    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("testTopic0");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    JSONObject jsonObject = JSON.parseObject(response.getEntityAsText());
    System.out.println(jsonObject);
    Assert.assertEquals(jsonObject.getJSONObject("serverToNumPartitionsMapping").size(), 2);
    for (String server : jsonObject.getJSONObject("serverToNumPartitionsMapping").keySet()) {
      Assert.assertEquals(
          jsonObject.getJSONObject("serverToNumPartitionsMapping").getIntValue(server), 4);
    }
    Assert.assertEquals(jsonObject.getJSONObject("serverToPartitionMapping").size(), 2);
    for (String server : jsonObject.getJSONObject("serverToPartitionMapping").keySet()) {
      Assert.assertEquals(
          jsonObject.getJSONObject("serverToPartitionMapping").getJSONArray(server).size(), 4);
    }
    Assert.assertEquals(jsonObject.getString("topic"), "testTopic0");

    // Delete topic
    request =
        ControllerRequestURLBuilder.baseUrl(REQUEST_URL).getTopicDeleteRequestUrl("testTopic0");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully finished delete topic: testTopic0");
  }

  @Test
  public void testPost() {
    // Create topic
    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicCreationRequestUrl("testTopic1", 16);
    Response response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully add new topic: {topic: testTopic1, partition: 16, pipeline: null}");
    Assert.assertTrue(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic1"));

    // Create existed topic
    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicCreationRequestUrl("testTopic1", 16);
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.CLIENT_ERROR_NOT_FOUND);
    Assert.assertEquals(response.getEntityAsText(),
        "Failed to add new topic: testTopic1, it is already existed!");

    // Delete topic
    request =
        ControllerRequestURLBuilder.baseUrl(REQUEST_URL).getTopicDeleteRequestUrl("testTopic1");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully finished delete topic: testTopic1");

  }

  @Test
  public void testPut() {
    // Create topic
    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicCreationRequestUrl("testTopic2", 8);
    Response response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully add new topic: {topic: testTopic2, partition: 8, pipeline: null}");
    Assert.assertTrue(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic2"));

    // Expand topic
    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExpansionRequestUrl("testTopic2", 16);
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully expand topic: {topic: testTopic2, partition: 16, pipeline: null}");

    // Expand non-existed topic
    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExpansionRequestUrl("testTopic22", 16);
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.CLIENT_ERROR_NOT_FOUND);
    Assert.assertEquals(response.getEntityAsText(),
        "Failed to expand topic, topic: testTopic22 is not existed!");

    // Delete topic
    request =
        ControllerRequestURLBuilder.baseUrl(REQUEST_URL).getTopicDeleteRequestUrl("testTopic2");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully finished delete topic: testTopic2");

  }

  @Test
  public void testDelete() {
    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicCreationRequestUrl("testTopic3", 8);
    Response response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully add new topic: {topic: testTopic3, partition: 8, pipeline: null}");
    Assert.assertTrue(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic3"));

    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("testTopic3");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);

    request =
        ControllerRequestURLBuilder.baseUrl(REQUEST_URL).getTopicDeleteRequestUrl("testTopic3");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully finished delete topic: testTopic3");

    request =
        ControllerRequestURLBuilder.baseUrl(REQUEST_URL).getTopicDeleteRequestUrl("testTopic4");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.CLIENT_ERROR_NOT_FOUND);
    Assert.assertEquals(response.getEntityAsText(),
        "Failed to delete not existed topic: testTopic4");

    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExternalViewRequestUrl("testTopic3");
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.CLIENT_ERROR_NOT_FOUND);

  }
}
