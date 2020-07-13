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
package com.uber.stream.ureplicator.common.observer;

import com.uber.stream.kafka.mirrormaker.common.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import kafka.server.KafkaServerStartable;
import org.easymock.EasyMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestKafkaBrokerTopicObserver {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestKafkaBrokerTopicObserver.class);
  private static KafkaBrokerTopicObserver kafkaBrokerTopicObserver;
  private KafkaServerStartable kafkaStarter;
  private ObserverCallback observerCallback = EasyMock.createMock(ObserverCallback.class);

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
    KafkaUReplicatorMetricsReporter.init(null);
    kafkaBrokerTopicObserver = new KafkaBrokerTopicObserver("broker0", KafkaStarterUtils.DEFAULT_ZK_STR, 10,
        observerCallback);
    kafkaBrokerTopicObserver.start();
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }
  }

  @AfterTest
  public void shutdown() {
    LOGGER.info("Trying to shutdown");
    kafkaBrokerTopicObserver.shutdown();
    KafkaStarterUtils.stopServer(kafkaStarter);
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testKafkaBrokerTopicObserver() {
    Assert.assertEquals(kafkaBrokerTopicObserver.getNumTopics(), 1);
    Assert.assertEquals(kafkaBrokerTopicObserver.getTopicPartition("testTopic0").getPartition(), 1);

    int topicCount = 3;
    for (int i = 1; i < topicCount; ++i) {
      String topicName = "testTopic" + i;
      // Create Kafka topic
      KafkaStarterUtils.createTopic(topicName, KafkaStarterUtils.DEFAULT_ZK_STR);
      try {
        Thread.sleep(100);
      } catch (Exception e) {
      }
      Assert.assertEquals(kafkaBrokerTopicObserver.getNumTopics(), 1 + i);
      for (int j = 0; j <= i; ++j) {
        Assert.assertTrue(kafkaBrokerTopicObserver.getAllTopics().contains("testTopic" + j));
        Assert.assertEquals(kafkaBrokerTopicObserver.getTopicPartition("testTopic" + j).getPartition(), 1);
      }
    }
  }

  @Test
  public void testPartitionChangeWatcherCallback() throws InterruptedException {
    String topicName = "testTopicExpansionWatcher";
    KafkaStarterUtils.createTopic(topicName, KafkaStarterUtils.DEFAULT_ZK_STR);

    observerCallback.onPartitionNumberChange(topicName, 4);
    EasyMock.expectLastCall().once();

    EasyMock.replay(observerCallback);

    Thread.sleep(100);
    kafkaBrokerTopicObserver.registerPartitionChangeWatcher(topicName);

    KafkaStarterUtils.expandTopic(topicName, 4, KafkaStarterUtils.DEFAULT_ZK_STR);
    Thread.sleep(100);

    int partitionCount = kafkaBrokerTopicObserver.getPartitionCount(topicName);
    Assert.assertEquals(partitionCount, 4);

    kafkaBrokerTopicObserver.unsubscribePartitionChangeWatcher(topicName);

    KafkaStarterUtils.expandTopic(topicName, 8, KafkaStarterUtils.DEFAULT_ZK_STR);
    Thread.sleep(100);
    partitionCount = kafkaBrokerTopicObserver.getPartitionCount(topicName);
    Assert.assertEquals(partitionCount, 8);

    EasyMock.verify(observerCallback);
  }
}
