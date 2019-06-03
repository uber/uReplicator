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

import com.uber.stream.kafka.mirrormaker.common.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import kafka.common.TopicAndPartition;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class CompactConsumerFetcherThreadTest {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CompactConsumerFetcherThreadTest.class);

  private KafkaServerStartable kafka;
  private final int clusterPort = 19092;
  private final String bootstrapServer = "localhost:" + clusterPort;
  private final String clusterZk = ZkStarter.DEFAULT_ZK_STR + "/" + clusterPort;
  private final String testTopic1 = "CompactConsumerFetcherThreadTest1";
  private final String testTopic2 = "CompactConsumerFetcherThreadTest2";

  @BeforeTest
  public void setup() {
    ZkStarter.startLocalZkServer();
    kafka = KafkaStarterUtils
        .startServer(clusterPort, KafkaStarterUtils.DEFAULT_BROKER_ID, clusterZk,
            KafkaStarterUtils.getDefaultKafkaConfiguration());
    kafka.startup();
  }

  @AfterTest
  public void shutdown() {
    kafka.shutdown();
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testCompactConsumerFetcherThread() throws InterruptedException, IOException {
    String threadName = "testCompactConsumerFetcherThread";
    ConcurrentHashMap<TopicAndPartition, PartitionOffsetInfo> partitionInfoMap = new ConcurrentHashMap<>();
    KafkaStarterUtils.createTopic(testTopic1, clusterZk);
    KafkaStarterUtils.createTopic(testTopic2, clusterZk);

    BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<>(3);
    CustomizedConsumerConfig properties = new CustomizedConsumerConfig(Utils.loadProps("src/test/resources/consumer.properties"));
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


    ConsumerFetcherThread fetcherThread = new ConsumerFetcherThread(threadName,
        properties);
    fetcherThread.start();

    TestUtils.produceMessages(bootstrapServer, testTopic1, 10);
    TestUtils.produceMessages(bootstrapServer, testTopic2, 10);

    Map<TopicPartition, PartitionOffsetInfo> addPartitions = new HashMap<>();
    TopicPartition tp1 = new TopicPartition(testTopic1, 0);
    PartitionOffsetInfo pi1 = new PartitionOffsetInfo(tp1,0L, null, queue);
    addPartitions.put(tp1, pi1);
    TopicPartition tp2 = new TopicPartition(testTopic2, 0);
    PartitionOffsetInfo pi2 = new PartitionOffsetInfo(tp2,5L, null, queue);
    addPartitions.put(tp2, pi2);

    fetcherThread.addPartitions(addPartitions);
    Thread.sleep(1000);
    Assert.assertEquals(1, queue.remainingCapacity());
    FetchedDataChunk record1 = queue.take();
    FetchedDataChunk record2 = queue.take();
    Assert.assertEquals(15, record1.messageSize() + record2.messageSize());

    addPartitions.remove(tp1);
    fetcherThread.addPartitions(addPartitions);
    Thread.sleep(1000);

    Assert.assertEquals(3, queue.remainingCapacity());

    // make sure testtopic1 still in fetcher thread
    com.uber.stream.ureplicator.worker.TestUtils.produceMessages(bootstrapServer, testTopic1, 10);
    Thread.sleep(1000);

    Assert.assertEquals(2, queue.remainingCapacity());
    FetchedDataChunk records = queue.take();
    Assert.assertEquals(10, records.messageSize());

    LOGGER.info("removePartitions/addPartitions to test specify same start offset again");
    Set<TopicPartition> removePartitions = new HashSet<>();
    removePartitions.add(tp2);
    fetcherThread.removePartitions(removePartitions);
    Thread.sleep(1000);

    fetcherThread.addPartitions(addPartitions);
    Thread.sleep(1000);

    Assert.assertEquals(2, queue.remainingCapacity());
    records = queue.take();
    Assert.assertEquals(5, records.messageSize());

    LOGGER.info("removePartitions/addPartitions to test out of range");
    PartitionOffsetInfo pi3 = new PartitionOffsetInfo(tp2, 15L, null, queue);
    addPartitions.remove(tp1);
    addPartitions.put(tp2, pi3);

    fetcherThread.removePartitions(removePartitions);
    //fetcherThread.doWork();
    Thread.sleep(1000);

    fetcherThread.addPartitions(addPartitions);
    //fetcherThread.doWork();
    Thread.sleep(1000);

    Assert.assertEquals(2, queue.remainingCapacity());
    records = queue.take();
    Assert.assertEquals(10, records.messageSize());

    // make sure testtopic1 still in fetcher thread
    com.uber.stream.ureplicator.worker.TestUtils.produceMessages(bootstrapServer, testTopic1, 10);
    //fetcherThread.doWork();
    Thread.sleep(1000);

    Assert.assertEquals(2, queue.remainingCapacity());
    records = queue.take();
    Assert.assertEquals(10, records.messageSize());
    fetcherThread.shutdown();
  }
}
