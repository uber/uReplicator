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


import com.uber.stream.ureplicator.common.observer.HeaderWhitelistObserver;
import com.uber.stream.ureplicator.common.observer.TopicPartitionCountObserver;
import com.uber.stream.ureplicator.worker.interfaces.IMessageTransformer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default message transformer converts ConsumerRecord to ProducerRecord
 */
public class DefaultMessageTransformer implements IMessageTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultMessageTransformer.class);
  private final TopicPartitionCountObserver topicPartitionCountObserver;
  private final HeaderWhitelistObserver headerWhitelistObserver;
  private final Map<String, String> topicMapping;

  public DefaultMessageTransformer(TopicPartitionCountObserver observer,
      HeaderWhitelistObserver headerWhitelistObserver,
      Map<String, String> topicMapping) {
    this.topicPartitionCountObserver = observer;
    this.headerWhitelistObserver = headerWhitelistObserver;
    this.topicMapping = topicMapping != null ? topicMapping : new HashMap<>();
  }

  @Override
  public ProducerRecord process(ConsumerRecord record) {
    String topic = topicMapping.getOrDefault(record.topic(), record.topic());
    int partitionCount = 0;
    if (topicPartitionCountObserver != null) {
      partitionCount = topicPartitionCountObserver.getPartitionCount(topic);
    }
    Integer partition =
        partitionCount > 0 && record.partition() >= 0 ? record.partition() % partitionCount : null;
    Long timpstamp = record.timestamp() <= 0 ? null : record.timestamp();
    if (headerWhitelistObserver != null && headerWhitelistObserver.getWhitelist().size() != 0) {
      Headers headers = getWhitelistedHeaders(record.headers(), headerWhitelistObserver.getWhitelist());
      return new ProducerRecord(topic, partition, timpstamp,
          record.key(),
          record.value(), headers);
    } else {
      return new ProducerRecord(topic, partition, timpstamp,
          record.key(),
          record.value(), record.headers());
    }
  }

  /**
   * Gets headers that is in the whitelist
   * @param originalHeaders original headers from consumer record
   * @param headerWhitelist header whitelist
   * @return headers in the whitelist
   */
  private Headers getWhitelistedHeaders(Headers originalHeaders, Set<String> headerWhitelist) {
    List<Header> whitelistedHeaders = new ArrayList<>();
    if (headerWhitelist == null || headerWhitelist.size() == 0) {
      return new RecordHeaders(whitelistedHeaders);
    }
    for (Header header : originalHeaders.toArray()) {
      if (headerWhitelist.contains(header.key())) {
        whitelistedHeaders.add(header);
      }
    }
    return new RecordHeaders(whitelistedHeaders);
  }
}
