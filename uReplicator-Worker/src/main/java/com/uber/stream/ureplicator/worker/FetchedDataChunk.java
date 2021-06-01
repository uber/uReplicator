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

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * consumer record chunk
 */
public class FetchedDataChunk {

  private final List<ConsumerRecord> records;
  private final PartitionOffsetInfo partitionOffsetInfo;
  private final int messageSize;

  public FetchedDataChunk(PartitionOffsetInfo partitionOffsetInfo, List<ConsumerRecord> records) {
    this.partitionOffsetInfo = partitionOffsetInfo;
    this.records = records;
    this.messageSize = records.size();
  }

  public PartitionOffsetInfo partitionOffsetInfo() {
    return partitionOffsetInfo;
  }

  public List<ConsumerRecord> consumerRecords() {
    return records;
  }

  public int messageSize() {
    return this.messageSize;
  }
}
