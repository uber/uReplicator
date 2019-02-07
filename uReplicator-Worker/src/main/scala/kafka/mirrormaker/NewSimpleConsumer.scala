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

import com.uber.kafka.consumer.NewSimpleConsumerConfig
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition

class NewSimpleConsumer(val host: String, val port: Int, val config: NewSimpleConsumerConfig) {
  private val underlying = new com.uber.kafka.consumer.NewSimpleConsumer(host, port, config)

  def connect(): Unit = {
    underlying.connect()
  }

  def fetch(requestBuilder: org.apache.kafka.common.requests.FetchRequest.Builder): FetchResponse = {
    new FetchResponse(underlying.fetch(requestBuilder))
  }

  def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val topicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
    underlying.earliestOrLatestOffset(topicPartition, earliestOrLatest)
  }

  def close(): Unit = {
    underlying.disconnect()
  }
}
