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

import com.uber.stream.kafka.mirrormaker.common.Constants;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerRequestURLBuilder;
import org.easymock.EasyMock;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Protocol;
import org.restlet.data.Status;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

public class TopicPartitionBlacklistRestletResourceTest {
  private final String CONTROLLER_PORT = "9999";
  private final String REQUEST_URL = "http://localhost:" + CONTROLLER_PORT;

  private final Component _component = new Component();
  private final Context applicationContext = _component.getContext().createChildContext();
  private final HelixMirrorMakerManager _helixMirrorMakerManager = EasyMock.createMock(HelixMirrorMakerManager.class);
  private final ControllerRestApplication _controllerRestApp = new ControllerRestApplication(null);

  @BeforeTest
  public void setup() throws Exception {
    applicationContext.getAttributes().put(HelixMirrorMakerManager.class.toString(),
        _helixMirrorMakerManager);

    _controllerRestApp.setContext(applicationContext);
    _component.getServers().add(Protocol.HTTP, Integer.parseInt(CONTROLLER_PORT));
    _component.getClients().add(Protocol.FILE);
    _component.getDefaultHost().attach(_controllerRestApp);
    _component.start();
  }

  @Test
  public void testGet() {
    EasyMock.reset(_helixMirrorMakerManager);
    TopicPartition tp = new TopicPartition("test", 3);
    Set<TopicPartition> blacklist = new HashSet<>();
    blacklist.add(tp);
    EasyMock.expect(_helixMirrorMakerManager.getTopicPartitionBlacklist()).andReturn(null).andReturn(blacklist);
    EasyMock.replay(_helixMirrorMakerManager);

    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getBlacklistRequestUrl();
    Response response = RestTestBase.HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(), "{}");

    response = RestTestBase.HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(), "{\"blacklist\":[{\"partition\":3,\"topic\":\"test\"}]}");
  }

  @Test
  public void testPost() {
    EasyMock.reset(_helixMirrorMakerManager);

    // input validation
    Request request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .postBlacklistRequestUrl("testTopic", "2", null);
    Response response = RestTestBase.HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.CLIENT_ERROR_BAD_REQUEST);
    Assert.assertEquals(response.getEntityAsText(), "Invalid parameter opt");


    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .postBlacklistRequestUrl("testTopic", "t", null);
    response = RestTestBase.HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.CLIENT_ERROR_BAD_REQUEST);
    Assert.assertEquals(response.getEntityAsText(), "Parameter partition should be Integer");


    _helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker("testTopic", 2, Constants.HELIX_ONLINE_STATE);
    EasyMock.expectLastCall();

    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .postBlacklistRequestUrl("testTopic", "2", "whitelist");
    response = RestTestBase.HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(), "OK");

    _helixMirrorMakerManager.updateTopicPartitionStateInMirrorMaker("testTopic", 3,Constants.HELIX_OFFLINE_STATE);
    EasyMock.expectLastCall();

    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .postBlacklistRequestUrl("testTopic", "3", "blacklist");
    response = RestTestBase.HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(), "OK");
  }

  @AfterTest
  public void cleanup() throws Exception {
    _controllerRestApp.stop();
    _component.stop();
  }
}
