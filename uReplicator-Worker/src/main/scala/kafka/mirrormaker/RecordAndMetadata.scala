package kafka.mirrormaker

import kafka.message.Message
import kafka.serializer.Decoder
import org.apache.kafka.common.record.{Record, TimestampType}
import org.apache.kafka.common.utils.Utils

case class RecordAndMetadata[K, V](topic: String,
                              partition: Int,
                              private val record: Record,
                              offset: Long,
                              keyDecoder: Decoder[K], valueDecoder: Decoder[V],
                              timestamp: Long = Message.NoTimestamp,
                              timestampType: TimestampType = TimestampType.CREATE_TIME) {

  /**
    * Return the decoded message key and payload
    */
  def key(): K = if(record.key == null) null.asInstanceOf[K] else keyDecoder.fromBytes(Utils.readBytes(record.key))

  def message(): V = if(record == null) null.asInstanceOf[V] else valueDecoder.fromBytes(Utils.readBytes(record.value))

}
