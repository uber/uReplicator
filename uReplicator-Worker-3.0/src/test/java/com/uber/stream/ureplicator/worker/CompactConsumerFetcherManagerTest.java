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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.collections.ExtendedProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class CompactConsumerFetcherManagerTest {

  private String testTopic1 = "CompactConsumerFetcherManagerTest1";
  List<PartitionInfo> partitionInfo1 = new ArrayList<>();
  private String testTopic2 = "CompactConsumerFetcherManagerTest2";
  List<PartitionInfo> partitionInfo2 = new ArrayList<>();
  private Map<TopicPartition, Integer> mockPartitionLeaderMap = new HashMap<>();
  private Map<String, ConsumerFetcherThread> fetcherThreadMap = new HashMap<>();
  private final Consumer mockConsumer = EasyMock.createMock(KafkaConsumer.class);
  private final ConsumerFetcherThread mockFetcherThread1 = EasyMock.createMock(ConsumerFetcherThread.class);
  private final ConsumerFetcherThread mockFetcherThread2 = EasyMock.createMock(ConsumerFetcherThread.class);
  private FetcherManagerGroupByLeaderId manager;
  @BeforeTest
  public void setup() {
    partitionInfo1.add(new PartitionInfo(testTopic1, 0, new Node(0,"localhost", 9092), null, null));
    partitionInfo1.add(new PartitionInfo(testTopic1, 1, new Node(1,"localhost", 9092), null, null));
    partitionInfo1.add(new PartitionInfo(testTopic1, 2, new Node(2,"localhost", 9092), null, null));
    partitionInfo1.add(new PartitionInfo(testTopic1, 3, new Node(0,"localhost", 9092), null, null));

    partitionInfo2.add(new PartitionInfo(testTopic2, 0, new Node(1,"localhost", 9092), null, null));
    partitionInfo2.add(new PartitionInfo(testTopic2, 1, new Node(2,"localhost", 9092), null, null));
    partitionInfo2.add(new PartitionInfo(testTopic2, 2, new Node(0,"localhost", 9092), null, null));
    partitionInfo2.add(new PartitionInfo(testTopic2, 3, new Node(1,"localhost", 9092), null, null));

    fakePartitionLeaderMap(partitionInfo1);
    fakePartitionLeaderMap(partitionInfo2);

    manager = new FetcherManagerGroupByLeaderId("CompactConsumerFetcherManagerTest", new CustomizedConsumerConfig(new Properties()), fetcherThreadMap, mockConsumer);
    fetcherThreadMap.put("CompactConsumerFetcherThread-0-2", mockFetcherThread1);
    fetcherThreadMap.put("CompactConsumerFetcherThread-2-0", mockFetcherThread2);

  }

  private void fakePartitionLeaderMap(List<PartitionInfo> partitionInfoList) {
    for (PartitionInfo partitionInfo : partitionInfoList) {
      mockPartitionLeaderMap.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), partitionInfo.leader().id());
    }
  }

  @Test
  public void testConsumerFetcherManagerGroupByLeaderId() throws InterruptedException {
    TopicPartition tp1 = new TopicPartition(testTopic1, 0);
    TopicPartition tp2 = new TopicPartition(testTopic2, 1);
    BlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<>(3);

    PartitionOffsetInfo partitionOffsetInfo1 = new PartitionOffsetInfo(tp1, 0L, 10L, queue);
    PartitionOffsetInfo partitionOffsetInfo2 = new PartitionOffsetInfo(tp2, 0L, 10L, queue);


    EasyMock.reset(mockConsumer, mockFetcherThread1, mockFetcherThread2);
    Map<String, List<PartitionInfo>> partitionInfoMap = new HashMap<>();
    partitionInfoMap.put(testTopic1, partitionInfo1);
    partitionInfoMap.put(testTopic2, partitionInfo2);
    EasyMock.expect(mockConsumer.listTopics()).andReturn(partitionInfoMap).anyTimes();
    mockConsumer.close();
    EasyMock.expectLastCall().once();
    mockFetcherThread1.addPartitions(ImmutableMap.of(tp1, partitionOffsetInfo1));
    EasyMock.expectLastCall().once();

    mockFetcherThread2.addPartitions(ImmutableMap.of(tp2, partitionOffsetInfo2));
    EasyMock.expectLastCall().once();

    mockFetcherThread1.removePartitions(ImmutableSet.of(tp1));
    EasyMock.expectLastCall().once();

    mockFetcherThread2.removePartitions(ImmutableSet.of(tp1));
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockFetcherThread1.getTopicPartitions()).andReturn(new HashSet<>());
    EasyMock.expect(mockFetcherThread2.getTopicPartitions()).andReturn(new HashSet<>());

    mockFetcherThread1.shutdown();
    EasyMock.expectLastCall().once();

    mockFetcherThread2.shutdown();
    EasyMock.expectLastCall().once();

    EasyMock.replay(mockConsumer, mockFetcherThread1, mockFetcherThread2);

    manager.start();

    manager.addTopicPartition(tp1, partitionOffsetInfo1);
    manager.addTopicPartition(tp2, partitionOffsetInfo2);

    manager.doWork();
    Assert.assertEquals(manager.getTopicPartitions().size(), 2);

    manager.removeTopicPartition(tp1);
    manager.doWork();
    Assert.assertEquals(manager.getTopicPartitions().size(), 1);

    manager.shutdown();
    EasyMock.verify(mockConsumer, mockFetcherThread1, mockFetcherThread2);
  }
}
