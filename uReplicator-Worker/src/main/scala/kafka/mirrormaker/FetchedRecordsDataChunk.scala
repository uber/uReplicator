package kafka.mirrormaker

import org.apache.kafka.common.record.Records

case class FetchedRecordsDataChunk(records: Records,
                              topicInfo: PartitionTopicInfo2,
                              fetchOffset: Long)
