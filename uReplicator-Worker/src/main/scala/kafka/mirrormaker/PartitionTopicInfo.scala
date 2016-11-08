/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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

import java.util.concurrent._
import java.util.concurrent.atomic._

import kafka.consumer.FetchedDataChunk
import kafka.utils.Logging

/**
 * PartitionTopicInfo contains the information for each topic partition
 *
 * @param topic
 * @param partitionId
 * @param chunkQueue
 * @param consumedOffset
 * @param fetchedOffset
 * @param fetchSize
 * @param clientId
 */
class PartitionTopicInfo(override val topic: String,
                         override val partitionId: Int,
                         private val chunkQueue: BlockingQueue[FetchedDataChunk],
                         private val consumedOffset: AtomicLong,
                         private val fetchedOffset: AtomicLong,
                         private val fetchSize: AtomicInteger,
                         private val clientId: String)
  extends kafka.consumer.PartitionTopicInfo(topic = topic,
                             partitionId = partitionId,
                             chunkQueue = chunkQueue,
                             consumedOffset = consumedOffset,
                             fetchedOffset = fetchedOffset,
                             fetchSize = fetchSize,
                             clientId = clientId) with Logging {

  val deleted:AtomicBoolean = new AtomicBoolean(false)
  def getDeleted() = deleted.get()
}
