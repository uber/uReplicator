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
package com.uber.stream.kafka.mirrormaker.manager.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.modules.ControllerWorkloadInfo;
import com.uber.stream.kafka.mirrormaker.common.utils.HttpClientUtils;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerWorkloadSnapshot;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ControllerWorkloadSnapshotTest {

  private ControllerWorkloadSnapshot controllerWorkloadSnapshot;
  private CloseableHttpClient httpClient;
  private RequestConfig requestConfig;

  @BeforeClass
  public void setup() {
    KafkaUReplicatorMetricsReporter.init(null);
    requestConfig = EasyMock.createMock(RequestConfig.class);
    httpClient = EasyMock.createMock(CloseableHttpClient.class);
    controllerWorkloadSnapshot = new ControllerWorkloadSnapshot(httpClient, requestConfig);
  }

  @Test
  public void testControllerWorkloadSnapshot() throws IOException, URISyntaxException {
    ControllerWorkloadInfo controllerWorkloadInfo = new ControllerWorkloadInfo();
    controllerWorkloadInfo.setAutoBalancingEnabled(true);
    controllerWorkloadInfo.setTopicWorkload(new TopicWorkload(11, 11, 0));
    EasyMock
        .expect(httpClient.execute(EasyMock.anyObject(HttpUriRequest.class), EasyMock.anyObject(ResponseHandler.class)))
        .andReturn(JSONObject.toJSONString(controllerWorkloadInfo)).times(1);

    EasyMock.replay(httpClient);
    controllerWorkloadSnapshot
        .updatePipelineHostInfoMap(ImmutableMap.of("test1", HostAndPort.fromParts("localhost", 5432)));
    controllerWorkloadSnapshot.refreshWorkloadInfo();
    Assert.assertEquals(controllerWorkloadSnapshot.getPipelineWorkloadMap().size(), 1);
    Assert.assertTrue(controllerWorkloadSnapshot.getPipelineWorkloadMap().containsKey("test1"));
    Assert.assertEquals(controllerWorkloadSnapshot.getPipelineWorkloadMap().get("test1"), controllerWorkloadInfo);

    controllerWorkloadSnapshot.refreshWorkloadInfo();
    Assert.assertEquals(controllerWorkloadSnapshot.getPipelineWorkloadMap().size(), 1);
    Assert.assertTrue(controllerWorkloadSnapshot.getPipelineWorkloadMap().containsKey("test1"));
    Assert.assertEquals(controllerWorkloadSnapshot.getPipelineWorkloadMap().get("test1"), controllerWorkloadInfo);
    Assert.assertEquals(controllerWorkloadSnapshot.getFailedPipelines().size(), 1);
    Assert.assertTrue(controllerWorkloadSnapshot.getFailedPipelines().contains("test1"));

    controllerWorkloadSnapshot
        .updatePipelineHostInfoMap(ImmutableMap.of("test2", HostAndPort.fromParts("localhost", 5431)));
    controllerWorkloadSnapshot.refreshWorkloadInfo();
    Assert.assertEquals(controllerWorkloadSnapshot.getPipelineWorkloadMap().size(), 0);
    Assert.assertEquals(controllerWorkloadSnapshot.getFailedPipelines().size(), 1);
    Assert.assertTrue(controllerWorkloadSnapshot.getFailedPipelines().contains("test2"));

  }

}
