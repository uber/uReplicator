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

import com.google.common.collect.ImmutableList;
import com.uber.stream.ureplicator.common.observer.TopicPartitionLeaderObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class KafkaClusterObserverTest {

  private final KafkaConsumer kafkaConsumer = EasyMock.createMock(KafkaConsumer.class);
  private final TopicPartitionLeaderObserver topicPartitionLeaderObserver = new TopicPartitionLeaderObserver(kafkaConsumer);
  private String testTopic1 = "KafkaClusterObserverTest1";
  private final List<PartitionInfo> partitionInfo1 = new ArrayList<>();
  private String testTopic2 = "KafkaClusterObserverTest2";
  private final List<PartitionInfo> partitionInfo2 = new ArrayList<>();
  private Map<TopicPartition, Integer> mockPartitionLeaderMap = new HashMap<>();

  @BeforeTest
  public void setup() {
    partitionInfo1
        .add(new PartitionInfo(testTopic1, 0, new Node(0, "localhost", 9092), null, null));
    partitionInfo1
        .add(new PartitionInfo(testTopic1, 1, new Node(1, "localhost", 9092), null, null));
    partitionInfo1
        .add(new PartitionInfo(testTopic1, 2, new Node(2, "localhost", 9092), null, null));
    partitionInfo1
        .add(new PartitionInfo(testTopic1, 3, new Node(0, "localhost", 9092), null, null));
    partitionInfo2
        .add(new PartitionInfo(testTopic2, 0, new Node(1, "localhost", 9092), null, null));
    partitionInfo2
        .add(new PartitionInfo(testTopic2, 1, new Node(2, "localhost", 9092), null, null));
    partitionInfo2
        .add(new PartitionInfo(testTopic2, 2, new Node(0, "localhost", 9092), null, null));
    partitionInfo2
        .add(new PartitionInfo(testTopic2, 3, new Node(1, "localhost", 9092), null, null));
  }

  @Test
  public void testKafkaClusterObserver() {
    EasyMock.reset(kafkaConsumer);

    TopicPartition tp1 = new TopicPartition(testTopic1, 1);
    TopicPartition tp2 = new TopicPartition(testTopic2, 1);
    Map<String, List<PartitionInfo>> partitionInfoMap = new HashMap<>();
    partitionInfoMap.put(testTopic1, partitionInfo1);
    partitionInfoMap.put(testTopic2, partitionInfo2);

    EasyMock.expect(kafkaConsumer.listTopics()).andReturn(partitionInfoMap);
    EasyMock.replay(kafkaConsumer);

    Map<TopicPartition, Integer> result =
        topicPartitionLeaderObserver.findLeaderForPartitions(ImmutableList.of(tp1, tp2));
    Assert.assertEquals(result.get(tp1), Integer.valueOf(1));
    Assert.assertEquals(result.get(tp2), Integer.valueOf(2));

    EasyMock.verify(kafkaConsumer);
  }

  @AfterTest
  public void shutdown() {
    partitionInfo1.clear();
    partitionInfo2.clear();
    mockPartitionLeaderMap.clear();
  }

}
