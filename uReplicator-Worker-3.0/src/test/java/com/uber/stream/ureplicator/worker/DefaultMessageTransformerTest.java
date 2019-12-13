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
import com.uber.stream.ureplicator.common.observer.HeaderWhitelistObserver;
import com.uber.stream.ureplicator.common.observer.TopicPartitionCountObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

public class DefaultMessageTransformerTest {

  private final String TOPIC = "DefaultMessageTransformerTest";
  private final String TOPIC_MIRROR = "DefaultMessageTransformerTest_mirror";

  private DefaultMessageTransformer defaultMessageTransformer;
  private TopicPartitionCountObserver topicPartitionCountObserver;
  private HeaderWhitelistObserver headerWhitelistObserver;
  private Map<String, String> topicMapping;
  private Map<String, String> MOCK_HEADERS = ImmutableMap
      .of("srcCluster", "test1", "destCluster", "test2");


  @Before
  public void setup() {
    topicPartitionCountObserver = EasyMock.createMock(TopicPartitionCountObserver.class);
    headerWhitelistObserver = EasyMock.createMock(HeaderWhitelistObserver.class);
    topicMapping = new HashMap<>();
  }

  private ConsumerRecord createConsumerRecord() {
    List<Header> headers = new ArrayList<>();
    for (Map.Entry<String, String> entry : MOCK_HEADERS.entrySet()) {
      headers.add(new RecordHeader(entry.getKey(), entry.getValue().getBytes()));
    }
    Headers recordHeaders = new RecordHeaders(headers);
    return new ConsumerRecord<>(TOPIC, 2, 44, 0,
        TimestampType.CREATE_TIME, 0L, 0, 0,
        "key".getBytes(), "value".getBytes(), recordHeaders);
  }

  @Test
  public void defaultMessageTransformerTest() {
    defaultMessageTransformer = new DefaultMessageTransformer(null, null, topicMapping);
    ProducerRecord record = defaultMessageTransformer.process(createConsumerRecord());
    Assert.assertEquals(record.topic(), TOPIC);
    Assert.assertEquals(record.partition(), null);
    Assert.assertEquals(record.headers().toArray().length, 2);

    topicMapping.put(TOPIC, TOPIC_MIRROR);
    EasyMock.expect(topicPartitionCountObserver.getPartitionCount(TOPIC_MIRROR)).andReturn(4);
    EasyMock.replay(topicPartitionCountObserver);
    EasyMock.expect(headerWhitelistObserver.getWhitelist())
        .andReturn(ImmutableSet.of());
    EasyMock.replay(headerWhitelistObserver);
    defaultMessageTransformer = new DefaultMessageTransformer(topicPartitionCountObserver,
        headerWhitelistObserver, topicMapping);
    record = defaultMessageTransformer.process(createConsumerRecord());
    Assert.assertEquals(record.topic(), TOPIC_MIRROR);
    Assert.assertEquals(record.partition(), new Integer(2));
    Assert.assertEquals(record.headers().toArray().length, 0);
  }

  @Test
  public void defaultMessageTransformerTestWithTopicMapping() {
    defaultMessageTransformer = new DefaultMessageTransformer(null, null, topicMapping);
    topicMapping.put(TOPIC, TOPIC_MIRROR);
    ProducerRecord record = defaultMessageTransformer.process(createConsumerRecord());
    Assert.assertEquals(record.topic(), TOPIC_MIRROR);
    Assert.assertEquals(record.partition(), null);
    Assert.assertEquals(record.headers().toArray().length, 2);
  }

  @Test
  public void defaultMessageTransformerTestWithTopicPartitionCountObserver() {
    defaultMessageTransformer = new DefaultMessageTransformer(topicPartitionCountObserver, null,
        topicMapping);
    EasyMock.expect(topicPartitionCountObserver.getPartitionCount(TOPIC)).andReturn(4);
    EasyMock.replay(topicPartitionCountObserver);
    ProducerRecord record = defaultMessageTransformer.process(createConsumerRecord());
    Assert.assertEquals(record.topic(), TOPIC);
    Assert.assertEquals(record.partition(), new Integer(2));
    Assert.assertEquals(record.headers().toArray().length, 2);
  }

  @Test
  public void defaultMessageTransformerTestWithHeaderWhitelist() {
    defaultMessageTransformer = new DefaultMessageTransformer(null, headerWhitelistObserver,
        topicMapping);
    EasyMock.expect(headerWhitelistObserver.getWhitelist())
        .andReturn(ImmutableSet.of("srcCluster")).times(2);
    EasyMock.replay(headerWhitelistObserver);
    ProducerRecord record = defaultMessageTransformer.process(createConsumerRecord());
    Assert.assertEquals(record.topic(), TOPIC);
    Assert.assertEquals(record.partition(), null);
    Assert.assertEquals(record.headers().toArray().length, 1);

    EasyMock.reset(headerWhitelistObserver);

    EasyMock.expect(headerWhitelistObserver.getWhitelist())
        .andReturn(ImmutableSet.of()).times(2);
    EasyMock.replay(headerWhitelistObserver);
    record = defaultMessageTransformer.process(createConsumerRecord());
    Assert.assertEquals(record.topic(), TOPIC);
    Assert.assertEquals(record.partition(), null);
    Assert.assertEquals(record.headers().toArray().length, 2);
  }
}
