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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.uber.stream.ureplicator.common.KafkaClusterObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FetcherManagerTest {

  private String testTopic1 = "ConsumerFetcherManagerTest1";
  private String testTopic2 = "ConsumerFetcherManagerTest2";
  private Map<String, ConsumerFetcherThread> fetcherThreadMap = new HashMap<>();
  private final ConsumerFetcherThread mockFetcherThread1 = EasyMock
      .createMock(ConsumerFetcherThread.class);
  private final ConsumerFetcherThread mockFetcherThread2 = EasyMock
      .createMock(ConsumerFetcherThread.class);
  private KafkaClusterObserver kafkaClusterObserver = EasyMock
      .createMock(KafkaClusterObserver.class);
  private ConsumerFetcherThread mockConsumerFetcherThread = EasyMock
      .createMock(ConsumerFetcherThread.class);

  class MockFetcherManager extends FetcherManager {

    Set<String> clientId = new HashSet<>();

    public MockFetcherManager(String threadName,
        CustomizedConsumerConfig consumerProperties,
        List<BlockingQueue<FetchedDataChunk>> messageQueue) {
      super(threadName, consumerProperties, messageQueue);
    }

    protected MockFetcherManager(String threadName,
        CustomizedConsumerConfig consumerProperties,
        Map<String, ConsumerFetcherThread> fetcherThreadMap,
        List<BlockingQueue<FetchedDataChunk>> messageQueue) {
      super(threadName, consumerProperties, fetcherThreadMap, messageQueue);
    }

    @VisibleForTesting
    protected ConsumerFetcherThread createConsumerFetcherThread(String threadName,
        CustomizedConsumerConfig properties,
        RateLimiter rateLimiter, BlockingQueue<FetchedDataChunk> chunkQueue) {
      if (properties.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)) {
        clientId.add(properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
      }
      return mockConsumerFetcherThread;
    }
  }

  @Test
  public void testConsumerFetcherManagerGroupByLeaderId() {
    fetcherThreadMap.clear();
    fetcherThreadMap.put("ConsumerFetcherThread-0", mockFetcherThread1);
    fetcherThreadMap.put("ConsumerFetcherThread-1", mockFetcherThread2);
    List<BlockingQueue<FetchedDataChunk>> messageQueue = new ArrayList<>();
    FetcherManagerGroupByLeaderId fetcherByLeaderId = new FetcherManagerGroupByLeaderId(
        "CompactConsumerFetcherManagerTest",
        new CustomizedConsumerConfig(new Properties()), fetcherThreadMap, messageQueue,
        kafkaClusterObserver);

    TopicPartition tp1 = new TopicPartition(testTopic1, 0);
    TopicPartition tp2 = new TopicPartition(testTopic2, 1);
    BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<>(3);
    messageQueue.add(queue);

    PartitionOffsetInfo partitionOffsetInfo1 = new PartitionOffsetInfo(tp1, 0L, 10L);
    PartitionOffsetInfo partitionOffsetInfo2 = new PartitionOffsetInfo(tp2, 0L, 10L);

    EasyMock.reset(kafkaClusterObserver, mockFetcherThread1, mockFetcherThread2);
    EasyMock.expect(kafkaClusterObserver.findLeaderForPartitions(ImmutableList.of(tp1, tp2)))
        .andReturn(ImmutableMap.of(tp1, 0, tp2, 1));
    mockFetcherThread1.addPartitions(ImmutableMap.of(tp1, partitionOffsetInfo1));
    EasyMock.expectLastCall().once();

    mockFetcherThread2.addPartitions(ImmutableMap.of(tp2, partitionOffsetInfo2));
    EasyMock.expectLastCall().once();

    mockFetcherThread1.removePartitions(ImmutableSet.of(tp1));
    EasyMock.expectLastCall().once();

    mockFetcherThread2.removePartitions(ImmutableSet.of(tp1));
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockFetcherThread1.initiateShutdown()).andReturn(true);

    EasyMock.expect(mockFetcherThread2.initiateShutdown()).andReturn(true);

    mockFetcherThread1.awaitShutdown();
    EasyMock.expectLastCall().once();

    mockFetcherThread2.awaitShutdown();
    EasyMock.expectLastCall().once();

    EasyMock.replay(kafkaClusterObserver, mockFetcherThread1, mockFetcherThread2);

    fetcherByLeaderId.start();

    fetcherByLeaderId.addTopicPartition(tp1, partitionOffsetInfo1);
    fetcherByLeaderId.addTopicPartition(tp2, partitionOffsetInfo2);

    fetcherByLeaderId.doWork();
    Assert.assertEquals(fetcherByLeaderId.getTopicPartitions().size(), 2);

    fetcherByLeaderId.removeTopicPartition(tp1);
    fetcherByLeaderId.doWork();
    Assert.assertEquals(fetcherByLeaderId.getTopicPartitions().size(), 1);

    fetcherByLeaderId.shutdown();
    EasyMock.verify(kafkaClusterObserver, mockFetcherThread1, mockFetcherThread2);
  }


  @Test
  public void testConsumerFetcherManagerByHashId() throws InterruptedException {
    fetcherThreadMap.clear();
    fetcherThreadMap.put("ConsumerFetcherThread-0", mockFetcherThread1);
    fetcherThreadMap.put("ConsumerFetcherThread-1", mockFetcherThread2);
    List<BlockingQueue<FetchedDataChunk>> messageQueue = new ArrayList<>();
    FetcherManager fetcherManagerByHashId = new FetcherManager("FetcherManagerGroupByHashId",
        new CustomizedConsumerConfig(new Properties()), fetcherThreadMap, messageQueue);

    TopicPartition tp1 = new TopicPartition(testTopic1, 0);
    TopicPartition tp2 = new TopicPartition(testTopic2, 1);
    BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<>(3);
    messageQueue.add(queue);

    PartitionOffsetInfo partitionOffsetInfo1 = new PartitionOffsetInfo(tp1, 0L, 10L);
    PartitionOffsetInfo partitionOffsetInfo2 = new PartitionOffsetInfo(tp2, 0L, 10L);

    EasyMock.reset(mockFetcherThread1, mockFetcherThread2);

    mockFetcherThread1.addPartitions(ImmutableMap.of(tp1, partitionOffsetInfo1));
    EasyMock.expectLastCall().once();

    mockFetcherThread2.addPartitions(ImmutableMap.of(tp2, partitionOffsetInfo2));
    EasyMock.expectLastCall().once();

    mockFetcherThread1.removePartitions(ImmutableSet.of(tp1));
    EasyMock.expectLastCall().once();

    mockFetcherThread2.removePartitions(ImmutableSet.of(tp1));
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockFetcherThread1, mockFetcherThread2);

    fetcherManagerByHashId.start();

    fetcherManagerByHashId.addTopicPartition(tp1, partitionOffsetInfo1);
    fetcherManagerByHashId.addTopicPartition(tp2, partitionOffsetInfo2);

    fetcherManagerByHashId.doWork();
    Assert.assertEquals(fetcherManagerByHashId.getTopicPartitions().size(), 2);

    fetcherManagerByHashId.removeTopicPartition(tp1);
    fetcherManagerByHashId.doWork();
    Assert.assertEquals(fetcherManagerByHashId.getTopicPartitions().size(), 1);

    EasyMock.verify(mockFetcherThread1, mockFetcherThread2);
  }

  @Test
  public void testAddFetcherForTopicPartition() {
    CustomizedConsumerConfig consumerConfig = new CustomizedConsumerConfig(new Properties());
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "ureplicator");
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ureplicator");
    List<BlockingQueue<FetchedDataChunk>> messageQueue = new ArrayList<>();
    BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<>(3);
    messageQueue.add(queue);
    MockFetcherManager fetcherManagerByHashId = new MockFetcherManager(
        "FetcherManagerGroupByHashId",
        consumerConfig, messageQueue);
    TopicPartition tp1 = new TopicPartition(testTopic1, 0);
    TopicPartition tp2 = new TopicPartition(testTopic2, 1);
    PartitionOffsetInfo partitionOffsetInfo1 = new PartitionOffsetInfo(tp1, 0L, 10L);
    PartitionOffsetInfo partitionOffsetInfo2 = new PartitionOffsetInfo(tp2, 0L, 10L);

    EasyMock.reset(mockConsumerFetcherThread);

    mockConsumerFetcherThread.start();
    EasyMock.expectLastCall().times(2);

    mockConsumerFetcherThread.addPartitions(Collections.singletonMap(tp1, partitionOffsetInfo1));
    EasyMock.expectLastCall().times(1);
    mockConsumerFetcherThread.addPartitions(Collections.singletonMap(tp2, partitionOffsetInfo2));
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(mockConsumerFetcherThread);

    fetcherManagerByHashId
        .addFetcherForTopicPartition(tp1, partitionOffsetInfo1, "fetcherThread-1");
    fetcherManagerByHashId
        .addFetcherForTopicPartition(tp2, partitionOffsetInfo2, "fetcherThread-2");

    Assert.assertEquals(fetcherManagerByHashId.clientId.size(), 2);
    Assert.assertTrue(fetcherManagerByHashId.clientId.contains("ureplicator-1"));
    Assert.assertTrue(fetcherManagerByHashId.clientId.contains("ureplicator-2"));
    Assert.assertEquals(consumerConfig.getProperty(ConsumerConfig.CLIENT_ID_CONFIG), "ureplicator");

    EasyMock.verify(mockConsumerFetcherThread);


  }
}

