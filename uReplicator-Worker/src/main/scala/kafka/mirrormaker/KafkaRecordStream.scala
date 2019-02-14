package kafka.mirrormaker

import java.util.concurrent.BlockingQueue
import kafka.serializer.Decoder

class KafkaRecordStream[K,V](private val queue: BlockingQueue[FetchedRecordsDataChunk],
                        consumerTimeoutMs: Int,
                        private val keyDecoder: Decoder[K],
                        private val valueDecoder: Decoder[V],
                        val clientId: String)
  extends Iterable[RecordAndMetadata[K,V]] with java.lang.Iterable[RecordAndMetadata[K,V]] {

  private val iter: KafkaStreamIterator[K,V] =
    new KafkaStreamIterator[K,V](queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId)

  /**
    *  Create an iterator over messages in the stream.
    */
  def iterator: KafkaStreamIterator[K,V] = iter

  /**
    * This method clears the queue being iterated during the consumer rebalancing. This is mainly
    * to reduce the number of duplicates received by the consumer
    */
  def clear() {
    iter.clearCurrentChunk()
  }

  override def toString: String = {
    "%s kafka stream".format(clientId)
  }
}
