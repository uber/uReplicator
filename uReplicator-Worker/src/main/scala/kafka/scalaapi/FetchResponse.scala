package kafka.scalaapi
import kafka.message.{ByteBufferMessageSet, Message}
import kafka.api.FetchResponsePartitionData

import scala.collection.JavaConversions._
import util.control.Breaks._
import org.slf4j.{Logger, LoggerFactory}
import java.nio.charset.StandardCharsets

class FetchResponse(private val underlying: org.apache.kafka.common.requests.FetchResponse) {
  val logger1: Logger = LoggerFactory.getLogger(this.getClass)
  def data(): scala.collection.Seq[scala.Tuple2[kafka.common.TopicAndPartition, kafka.api.FetchResponsePartitionData]] = {
    var seq: scala.collection.Seq[scala.Tuple2[kafka.common.TopicAndPartition, kafka.api.FetchResponsePartitionData]] =
      scala.collection.Seq()
    val a = underlying.responseData()
    for (entry <- underlying.responseData().entrySet()) {
      val topicAndPartition = entry.getKey
      val partitionData = entry.getValue
      val a = kafka.common.TopicAndPartition(topicAndPartition.topic(), topicAndPartition.partition())

      var messages : List[Message] = List()

      val records = partitionData.records.records()

      var count = 0
      breakable {
        records.foreach {
          case (record) =>

            var key : Array[Byte] = null
            if (record.key() != null) {
              key = record.value().array()
            }

            val msg = new Message(record.value().array(), key, record.timestamp(), Message.CurrentMagicValue)
            val strMsg = new String(record.value().array(), StandardCharsets.UTF_8)
            messages = messages :+ msg
            count = count + 1
            if (count > 20) {
               break
            }
        }
      }

      val b : ByteBufferMessageSet = new ByteBufferMessageSet(messages:_*)
      val d = FetchResponsePartitionData(partitionData.error, partitionData.highWatermark, b)

      seq = seq :+ Tuple2(a, d)
      val c = seq.length
    }
    logger1.info(s"qwerty123456 seq length $seq")
    seq
  }
}
