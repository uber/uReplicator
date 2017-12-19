package kafka.offsetmanager

import java.util.concurrent.TimeUnit

import kafka.cluster.Broker
import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool

class KafkaOffsetManager(brokers : Seq[Broker],
                         groupId: String,
                         clientId: String,
                         socketTimeoutMs: Int,
                         backoffMs: Int,
                         maxRetries: Int) extends OffsetManager {

  private val kafkaOffsetManagerConnector = new KafkaOffsetManagerConnector(brokers, socketTimeoutMs, backoffMs, maxRetries)
  private val kafkaCommitMeter = KafkaMetricsGroup.newMeter("KafkaCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> clientId))

  /**
    * Commit to kafka all the uncommitted offsets for the consumerGroup.
    *
    * @param offsetsAtr - Map which contain topic, partition and the offset to commit.
    * @param checkpointedOffsets - Pool of the committed offsets.
    */
  override def commitOffsets(offsetsAtr: Map[TopicAndPartition, OffsetAndMetadata],
                             checkpointedOffsets: Pool[TopicAndPartition, Long]): Unit = {
    // Check if the offsets need to be updated
    val offsetsToCommit = offsetsAtr.filter(kafkaOffsetArr =>
      checkpointedOffsets.get(kafkaOffsetArr._1) != kafkaOffsetArr._2.offset)
    kafkaOffsetManagerConnector.commit(groupId, offsetsToCommit)
    offsetsToCommit.foreach(kafkaOffsetArr => checkpointedOffsets.put(kafkaOffsetArr._1, kafkaOffsetArr._2.offset))
    kafkaCommitMeter.mark(offsetsToCommit.size)
  }

  /**
    * Fetch the consumerGroup's offset of the requested topic and partition.
    *
    * @param topicAndPartition - The requested topic and partition for fetching.
    * @return - The requested topic, partition and the current offset of the consumerGroup.
    */
  override def fetchOffset(topicAndPartition: TopicAndPartition): (TopicAndPartition, OffsetMetadataAndError) =
    kafkaOffsetManagerConnector.fetch(groupId, topicAndPartition).get

}
