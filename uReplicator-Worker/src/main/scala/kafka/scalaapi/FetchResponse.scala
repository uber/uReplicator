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

import kafka.message.{ByteBufferMessageSet, Message, NoCompressionCodec}
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import kafka.common.TopicAndPartition
import kafka.api.FetchResponsePartitionData

import scala.collection.Seq
import org.apache.kafka.common.record.{CompressionType, LegacyRecord, TimestampType}
import org.slf4j.{Logger, LoggerFactory}

class FetchResponse(private val underlying: org.apache.kafka.common.requests.FetchResponse) {
  val logger1: Logger = LoggerFactory.getLogger(this.getClass)
  def data(): Seq[(TopicAndPartition, FetchResponsePartitionData)] = {
    var seq: Seq[(TopicAndPartition, FetchResponsePartitionData)] = Seq()
    for (entry <- underlying.responseData().entrySet()) {
      val topicAndPartition = entry.getKey
      val partitionData = entry.getValue

      var messages : List[Message] = List()

      var count = 0
      partitionData.records.records().foreach {
        case (record) =>
          var keyByteArr : Array[Byte] = null
          var valueByteArr : Array[Byte] = null
          if (record.key() != null) {
            keyByteArr = record.value().array()
          }
          if (record.value() != null) {
            valueByteArr = Utils.toArray(record.value())
          }
          val msg = new Message(
            valueByteArr,
            keyByteArr,
            record.timestamp(),
            TimestampType.CREATE_TIME,
            NoCompressionCodec,
            record.offset().toInt,
            -1,
            Message.CurrentMagicValue
          )
          messages = messages :+ msg
          count = count + 1
          val s = new String(valueByteArr)
          logger1.info(s"temp3 fetchresponse $count $s")
      }

      val responseDataPartition = FetchResponsePartitionData(
        partitionData.error,
        partitionData.highWatermark,
        new ByteBufferMessageSet(messages:_*)
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
