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
package kafka.scalaapi
import scala.collection.JavaConversions._
import kafka.common.TopicAndPartition

import scala.collection.Seq
import org.slf4j.{Logger, LoggerFactory}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records

case class FetchResponsePartitionData2(error: Errors = Errors.NONE, hw: Long = -1L, messages: Records) {

}

class FetchResponse(private val underlying: org.apache.kafka.common.requests.FetchResponse) {
  val logger1: Logger = LoggerFactory.getLogger(this.getClass)
  def data(): Seq[(TopicAndPartition, FetchResponsePartitionData2)] = {
    var seq: Seq[(TopicAndPartition, FetchResponsePartitionData2)] = Seq()
    for (entry <- underlying.responseData().entrySet()) {
      val topicAndPartition = entry.getKey
      val partitionData = entry.getValue

      val responseDataPartition = FetchResponsePartitionData2(
        partitionData.error,
        partitionData.highWatermark,
        partitionData.records
      )

      val topicPartition = TopicAndPartition(
        topicAndPartition.topic(),
        topicAndPartition.partition()
      )
      seq = seq :+ Tuple2(topicPartition, responseDataPartition)
    }
    seq
  }
}
