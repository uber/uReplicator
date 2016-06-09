/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.restlet.Client;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Protocol;
import org.restlet.data.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.ControllerStarter;
import com.uber.stream.kafka.mirrormaker.controller.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerRequestURLBuilder;
import com.uber.stream.kafka.mirrormaker.controller.utils.ControllerTestUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.FakeInstance;
import com.uber.stream.kafka.mirrormaker.controller.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.controller.utils.ZkStarter;

import kafka.server.KafkaServerStartable;

public class ControllerStarterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarterTest.class);
  public static String CONTROLLER_PORT = "9999";
  public static Client HTTP_CLIENT = new Client(Protocol.HTTP);
  public static ControllerStarter CONTROLLER_STARTER = null;
  public static String REQUEST_URL;
  public static String HELIX_CLUSTER_NAME = "ControllerStarterTest";
  public static List<FakeInstance> FAKE_INSTANCES = new ArrayList<FakeInstance>();
  public static ZkClient ZK_CLIENT = null;

  private static KafkaBrokerTopicObserver kafkaBrokerTopicObserver;
  private KafkaServerStartable kafkaStarter;

  @BeforeTest
  public void setup() {
    LOGGER.info("Trying to setup");
    ZkStarter.startLocalZkServer();
    kafkaStarter =
        KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_BROKER_ID,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    // Create Kafka topic
    KafkaStarterUtils.createTopic("testTopic0", KafkaStarterUtils.DEFAULT_ZK_STR);
    try {
      Thread.sleep(2000);
    } catch (Exception e) {
    }
    kafkaBrokerTopicObserver =
        new KafkaBrokerTopicObserver("broker0", KafkaStarterUtils.DEFAULT_ZK_STR);

    // Create Kafka topic
    KafkaStarterUtils.createTopic("testTopic0", KafkaStarterUtils.DEFAULT_ZK_STR);
    
    
    ZK_CLIENT = new ZkClient(ZkStarter.DEFAULT_ZK_STR);
    ZK_CLIENT.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    REQUEST_URL = "http://localhost:" + CONTROLLER_PORT;
    CONTROLLER_STARTER = startController(HELIX_CLUSTER_NAME, CONTROLLER_PORT);
    try {
      FAKE_INSTANCES.addAll(ControllerTestUtils
          .addFakeDataInstancesToAutoJoinHelixCluster(HELIX_CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR,
              4, 0));
      Thread.sleep(4000);
    } catch (Exception e) {
      throw new RuntimeException("Error during adding fake instances");
    }
  }

  @AfterTest
  public void shutdown() {
    LOGGER.info("Trying to shutdown");
    for (FakeInstance fakeInstance : FAKE_INSTANCES) {
      try {
        LOGGER.info("Trying to shutdown: " + fakeInstance);
        fakeInstance.stop();
      } catch (Exception e) {
      }
    }
    LOGGER.info("Trying to stop controller");
    CONTROLLER_STARTER.stop();
    LOGGER.info("Trying to stop zk");

    kafkaBrokerTopicObserver.stop();
    KafkaStarterUtils.stopServer(kafkaStarter);
    
    ZK_CLIENT.deleteRecursive("/" + HELIX_CLUSTER_NAME);
    ZK_CLIENT.close();
    ZkStarter.stopLocalZkServer();
  }

  public ControllerStarter startController(String helixClusterName, String port) {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerPort(port);
    conf.setZkStr(ZkStarter.DEFAULT_ZK_STR);
    conf.setHelixClusterName(helixClusterName);
    conf.setBackUpToGit("false");
    conf.setAutoRebalanceDelayInSeconds("0");
    conf.setEnableAutoWhitelist("true");
    conf.setEnableAutoTopicExpansion("true");
    conf.setSrcKafkaZkPath(KafkaStarterUtils.DEFAULT_ZK_STR);
    conf.setDestKafkaZkPath(KafkaStarterUtils.DEFAULT_ZK_STR);
    
    final ControllerStarter starter = new ControllerStarter(conf);
    try {
      starter.start();
    } catch (Exception e) {
      throw new RuntimeException("Cannot start Helix Mirror Maker Controller");
    }
    return starter;
  }

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
        "Successfully add new topic: {topic: testTopic0, partition: 8}");
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
    Assert.assertEquals(jsonObject.getJSONObject("serverToNumPartitionsMapping").size(), 4);
    for (String server : jsonObject.getJSONObject("serverToNumPartitionsMapping").keySet()) {
      Assert.assertEquals(
          jsonObject.getJSONObject("serverToNumPartitionsMapping").getIntValue(server), 2);
    }
    Assert.assertEquals(jsonObject.getJSONObject("serverToPartitionMapping").size(), 4);
    for (String server : jsonObject.getJSONObject("serverToPartitionMapping").keySet()) {
      Assert.assertEquals(
          jsonObject.getJSONObject("serverToPartitionMapping").getJSONArray(server).size(), 2);
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
        "Successfully add new topic: {topic: testTopic1, partition: 16}");
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
        "Successfully add new topic: {topic: testTopic2, partition: 8}");
    Assert.assertTrue(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic2"));

    // Expand topic
    request = ControllerRequestURLBuilder.baseUrl(REQUEST_URL)
        .getTopicExpansionRequestUrl("testTopic2", 16);
    response = HTTP_CLIENT.handle(request);
    Assert.assertEquals(response.getStatus(), Status.SUCCESS_OK);
    Assert.assertEquals(response.getEntityAsText(),
        "Successfully expand topic: {topic: testTopic2, partition: 16}");

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
        "Successfully add new topic: {topic: testTopic3, partition: 8}");
    Assert.assertTrue(ZK_CLIENT.exists("/" + HELIX_CLUSTER_NAME + "/CONFIGS/RESOURCE/testTopic3"));

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

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
