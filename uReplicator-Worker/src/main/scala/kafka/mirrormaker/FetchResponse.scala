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

import kafka.api.FetchResponsePartitionData
import kafka.common.TopicAndPartition
import kafka.message.{ByteBufferMessageSet, Message, NoCompressionCodec}
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import scala.collection.Seq

class FetchResponse(private val underlying: org.apache.kafka.common.requests.FetchResponse) {
  def data(): Seq[(TopicAndPartition, FetchResponsePartitionData)] = {
    var data: Seq[(TopicAndPartition, FetchResponsePartitionData)] = Seq()
    for (entry <- underlying.responseData().entrySet()) {
      val topicAndPartition = entry.getKey
      val partitionData = entry.getValue

      var messages: List[Message] = List()
      var offsets: List[Long] = List()

      partitionData.records.records().toSeq.foreach {
        case record: Record =>
          var keyByteArr: Array[Byte] = null
          var valueByteArr: Array[Byte] = null
          if (record.key() != null) {
            keyByteArr = Utils.toArray(record.key())
          }
          if (record.value() != null) {
            valueByteArr = Utils.toArray(record.value())
          }
          val msg = new Message(
            valueByteArr,
            keyByteArr,
            record.timestamp(),
            Message.CurrentMagicValue
          )
          messages = messages :+ msg
          offsets = offsets :+ record.offset()
      }
      val responseDataPartition = FetchResponsePartitionData(
        partitionData.error,
        partitionData.highWatermark,
        new ByteBufferMessageSet(NoCompressionCodec, offsets, messages:_*)
      )
      val topicPartition = TopicAndPartition(
        topicAndPartition.topic(),
        topicAndPartition.partition()
      )
      data = data :+ (topicPartition, responseDataPartition)
    }
    data
  }
}
