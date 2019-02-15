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

import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import kafka.consumer.ConsumerTopicStatsRegistry
import kafka.utils.Logging
import org.apache.kafka.common.record.Records

import scala.collection.JavaConverters._

object PartitionTopicInfo2 {
  val InvalidOffset = -1L

  def isOffsetInvalid(offset: Long) = offset < 0L
}

class PartitionTopicInfo2(val topic: String,
                          val partitionId: Int,
                          private val chunkQueue: BlockingQueue[FetchedRecordsDataChunk],
                          private val consumedOffset: AtomicLong,
                          private val fetchedOffset: AtomicLong,
                          private val fetchSize: AtomicInteger,
                          private val clientId: String) extends Logging {

  debug("initial consumer offset of " + this + " is " + consumedOffset.get)
  debug("initial fetch offset of " + this + " is " + fetchedOffset.get)

  val deleted: AtomicBoolean = new AtomicBoolean(false)

  private val consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId)

  def getConsumeOffset() = consumedOffset.get

  def getFetchOffset() = fetchedOffset.get

  def resetConsumeOffset(newConsumeOffset: Long) = {
    consumedOffset.set(newConsumeOffset)
    debug("reset consume offset of " + this + " to " + newConsumeOffset)
  }

  def resetFetchOffset(newFetchOffset: Long) = {
    fetchedOffset.set(newFetchOffset)
    debug("reset fetch offset of ( %s ) to %d".format(this, newFetchOffset))
  }

  /**
    * Enqueue a message set for processing.
    */
  def enqueue(records: Records) {
    val size = records.sizeInBytes
    if(size > 0) {
      val next = records.batches().iterator().asScala.toSeq.last.nextOffset()
      trace("Updating fetch offset = " + fetchedOffset.get + " to " + next)
      chunkQueue.put(new FetchedRecordsDataChunk(records, this, fetchedOffset.get))
      fetchedOffset.set(next)
      debug("updated fetch offset of (%s) to %d".format(this, next))
      consumerTopicStats.getConsumerTopicStats(topic).byteRate.mark(size)
      consumerTopicStats.getConsumerAllTopicStats().byteRate.mark(size)
    } else if(records.sizeInBytes > 0) {
      chunkQueue.put(new FetchedRecordsDataChunk(records, this, fetchedOffset.get))
    }
  }

  def getDeleted() = deleted.get()

  override def toString: String = topic + ":" + partitionId.toString + ": fetched offset = " + fetchedOffset.get +
    ": consumed offset = " + consumedOffset.get
}
