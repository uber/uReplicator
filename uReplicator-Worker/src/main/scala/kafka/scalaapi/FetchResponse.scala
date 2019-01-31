package kafka.scalaapi

import kafka.message.{ByteBufferMessageSet, Message}
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import kafka.common.TopicAndPartition
import kafka.api.FetchResponsePartitionData
import scala.collection.Seq

class FetchResponse(private val underlying: org.apache.kafka.common.requests.FetchResponse) {
  def data(): Seq[(TopicAndPartition, FetchResponsePartitionData)] = {
    var seq: Seq[(TopicAndPartition, FetchResponsePartitionData)] = Seq()
    for (entry <- underlying.responseData().entrySet()) {
      val topicAndPartition = entry.getKey
      val partitionData = entry.getValue

      var messages : List[Message] = List()

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
            Message.CurrentMagicValue
          )
          messages = messages :+ msg
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
