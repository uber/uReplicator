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

import com.uber.stream.kafka.mirrormaker.common.core.TopicPartitionCountObserver;
import com.uber.stream.ureplicator.worker.interfaces.IMessageTransformer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default message transformer converts ConsumerRecord to ProducerRecord
 */
public class DefaultMessageTransformer implements IMessageTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageTransformer.class);

  private final TopicPartitionCountObserver topicPartitionCountObserver;
  private final Map<String, String> topicMapping;

  public DefaultMessageTransformer(TopicPartitionCountObserver observer,
      Map<String, String> topicMapping) {
    this.topicPartitionCountObserver = observer;
    this.topicMapping = topicMapping != null ? topicMapping : new HashMap<>();
  }

  @Override
  public ProducerRecord process(ConsumerRecord record) {
    String topic = topicMapping.getOrDefault(record.topic(), record.topic());
//    int partitionCount = 0;
//    if (topicPartitionCountObserver != null) {
//      partitionCount = topicPartitionCountObserver.getPartitionCount(topic);
//    }

//  Integer partition = partitionCount > 0 && record.partition() >= 0 ? record.partition() % partitionCount : null;
    Integer partition = record.partition();
    Long timpstamp = record.timestamp() <= 0 ? null : record.timestamp();
    return new ProducerRecord(topic, partition, timpstamp,
        record.key(),
        record.value(), record.headers());

  }
}
