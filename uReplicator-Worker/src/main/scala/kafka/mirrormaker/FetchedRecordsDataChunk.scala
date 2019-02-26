/**
 * Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
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
package kafka.mirrormaker

import org.apache.kafka.common.record.Records

// This file is taken from https://github.com/apache/kafka/blob/1.1/core/src/main/scala/kafka/consumer/FetchedDataChunk.scala
// This helps to replace the scala based ByteBufferMessageSet with java Records.
case class FetchedRecordsDataChunk(records: Records,
                                   topicInfo: PartitionTopicInfo,
                                   fetchOffset: Long)
