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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.common.{ClientIdAndBroker, ErrorMapping, KafkaException, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.message.{ByteBufferMessageSet, InvalidMessageException, MessageAndOffset}
import kafka.server.{ClientIdTopicPartition, FetcherLagStats, FetcherStats, PartitionFetchState}
import kafka.utils.CoreUtils._
import kafka.utils.ShutdownableThread

import scala.collection.{Map, Set, mutable}

/**
 * Fetcher thread that fetches data for multiple topic partitions from the same broker.
 *
 * @param name
 * @param config
 * @param sourceBroker
 * @param partitionInfoMap
 * @param consumerFetcherManager
 */
class CompactConsumerFetcherThread(name: String,
                                   val config: ConsumerConfig,
                                   sourceBroker: BrokerEndPoint,
                                   partitionInfoMap: ConcurrentHashMap[TopicAndPartition, PartitionTopicInfo],
                                   consumerFetcherManager: CompactConsumerFetcherManager)
  extends ShutdownableThread(name, isInterruptible = true) {
  private val clientId = config.clientId
  private val socketTimeout = config.socketTimeoutMs
  private val socketBufferSize = config.socketReceiveBufferBytes
  private val fetchSize = config.fetchMessageMaxBytes
  private val fetcherBrokerId = Request.OrdinaryConsumerId
  private val maxWait = config.fetchWaitMaxMs
  private val minBytes = config.fetchMinBytes
  private val fetchBackOffMs = config.refreshLeaderBackoffMs

  private var lastDumpTime = 0L;
  private final val DUMP_INTERVAL_MS = 5*60*1000;

  private val partitionMap = new mutable.HashMap[TopicAndPartition, PartitionFetchState] // a (topic, partition) -> partitionFetchState map
  private val partitionAddMap = new ConcurrentHashMap[TopicAndPartition, PartitionFetchState]
  private val partitionDeleteMap = new ConcurrentHashMap[TopicAndPartition, Boolean]
  private val updateMapLock = new ReentrantLock
  private val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize, clientId)
  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)
  val fetchRequestBuilder = new FetchRequestBuilder().
    clientId(clientId).
    replicaId(fetcherBrokerId).
    maxWait(maxWait).
    minBytes(minBytes)

  var newMsgSize = 0

  def isValidTopicPartition(tp: TopicAndPartition): Boolean = {
    val pti = partitionInfoMap.get(tp)
    (pti != null) && (!pti.getDeleted())
  }

  // process fetched data
  def processPartitionData(topicAndPartition: TopicAndPartition, fetchOffset: Long, partitionData: FetchResponsePartitionData) {
    try {
      if (!isValidTopicPartition(topicAndPartition)) {
        // don't do anything
        return
      }

      val pti = partitionInfoMap.get(topicAndPartition)
      if (pti.getFetchOffset != fetchOffset)
        throw new RuntimeException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d"
          .format(topicAndPartition.topic, topicAndPartition.partition, pti.getFetchOffset, fetchOffset))
      pti.enqueue(partitionData.messages.asInstanceOf[ByteBufferMessageSet])
    } catch {
      case e: java.util.NoSuchElementException => {
        // don't do anything
        return
      }
    }
  }

  // handle a partition whose offset is out of range and return a new fetch offset
  def handleOffsetOutOfRange(topicAndPartition: TopicAndPartition): Long = {
    var startTimestamp : Long = 0
    config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => startTimestamp = OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => startTimestamp = OffsetRequest.LatestTime
      case _ => startTimestamp = OffsetRequest.LatestTime
    }
    val newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId)
    val pti = partitionInfoMap.get(topicAndPartition)
    pti.resetFetchOffset(newOffset)
    pti.resetConsumeOffset(newOffset)
    newOffset
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicAndPartition]) {
    removePartitions(partitions.toSet)
    consumerFetcherManager.addPartitionsWithError(partitions)
  }

  def logTopicPartitionInfo(): Unit ={
    if ((System.currentTimeMillis() - lastDumpTime) > DUMP_INTERVAL_MS) {
      info("Topic partitions dump in fetcher thread: %s".format(partitionMap.map{ case (topicAndPartition, partitionFetchState) =>
        "[" + topicAndPartition + ", Offset " + partitionFetchState.offset + "] "}))
      lastDumpTime = System.currentTimeMillis()
    }
  }

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()
    simpleConsumer.close()
  }

  override def doWork() {
    var fetchRequest: FetchRequest = null

    inLock(partitionMapLock) {
      inLock(updateMapLock) {
        // add topic partition into partitionMap
        val addIter = partitionAddMap.entrySet().iterator()
        while (addIter.hasNext) {
          val tpToAdd = addIter.next()
          if (!partitionMap.contains(tpToAdd.getKey)) {
            partitionMap.put(tpToAdd.getKey, tpToAdd.getValue)
          }
        }
        partitionAddMap.clear()

        // remove topic partition from partitionMap
        val deleteIter = partitionDeleteMap.entrySet().iterator()
        while (deleteIter.hasNext) {
          val tpToDelete = deleteIter.next()
          if (partitionMap.contains(tpToDelete.getKey)) {
            partitionMap.remove(tpToDelete.getKey)
          }
          val lagMetricToRemove = new ClientIdTopicPartition(clientId, tpToDelete.getKey.topic, tpToDelete.getKey.partition)
          if (fetcherLagStats.stats.contains(lagMetricToRemove)) {
            fetcherLagStats.stats.remove(lagMetricToRemove)
          }
        }
        partitionDeleteMap.clear()
      }

      partitionMap.foreach {
        case((topicAndPartition, partitionFetchState)) =>
          if(partitionFetchState.isActive)
            fetchRequestBuilder.addFetch(topicAndPartition.topic, topicAndPartition.partition,
              partitionFetchState.offset, fetchSize)
      }

      fetchRequest = fetchRequestBuilder.build()
      if (fetchRequest.requestInfo.isEmpty) {
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      logTopicPartitionInfo()
    }

    if(!fetchRequest.requestInfo.isEmpty)
      processFetchRequest(fetchRequest)
  }

  private def processFetchRequest(fetchRequest: FetchRequest) {
    val partitionsWithError = new mutable.HashSet[TopicAndPartition]
    var response: FetchResponse = null
    try {
      trace("Issuing to broker %d of fetch request %s".format(sourceBroker.id, fetchRequest))
      response = simpleConsumer.fetch(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning.get) {
          warn("Error in fetch %s. Possible cause: %s".format(fetchRequest, t.toString))
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionMap.keys
            // there is an error occurred while fetching partitions, sleep a while
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    fetcherStats.requestRate.mark()

    newMsgSize = 0
    if (response != null) {
      // process fetched data
      inLock(partitionMapLock) {
        response.data.foreach {
          case(topicAndPartition, partitionData) =>
            val (topic, partitionId) = topicAndPartition.asTuple
            partitionMap.get(topicAndPartition).foreach(currentPartitionFetchState =>
              // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
              if (fetchRequest.requestInfo(topicAndPartition).offset == currentPartitionFetchState.offset) {
                partitionData.error match {
                  case ErrorMapping.NoError =>
                    try {
                      val messages = partitionData.messages.asInstanceOf[ByteBufferMessageSet]
                      val validBytes = messages.validBytes
                      val newOffset = messages.shallowIterator.toSeq.lastOption match {
                        case Some(m: MessageAndOffset) => m.nextOffset
                        case None => currentPartitionFetchState.offset
                      }
                      partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                      fetcherLagStats.getFetcherLagStats(topic, partitionId).lag = partitionData.hw - newOffset
                      fetcherStats.byteRate.mark(validBytes)
                      // Once we hand off the partition data to processPartitionData, we don't want to mess with it any more in this thread
                      processPartitionData(topicAndPartition, currentPartitionFetchState.offset, partitionData)
                      debug("validBytes=%d, sizeInBytes=%d".format(messages.validBytes, messages.sizeInBytes))
                      newMsgSize += validBytes
                    } catch {
                      // TODO: add stats tracking for invalid messages
                      case ime: InvalidMessageException =>
                        // we log the error and continue. This ensures two things
                        // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                        // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                        // should get fixed in the subsequent fetches
                        logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.offset  + " error " + ime.getMessage)
                      case e: Throwable =>
                        throw new KafkaException("error processing data for partition [%s,%d] offset %d"
                          .format(topic, partitionId, currentPartitionFetchState.offset), e)
                    }
                  case ErrorMapping.OffsetOutOfRangeCode =>
                    try {
                      val newOffset = handleOffsetOutOfRange(topicAndPartition)
                      partitionMap.put(topicAndPartition, new PartitionFetchState(newOffset))
                      error("Current offset %d for partition [%s,%d] out of range; reset offset to %d"
                        .format(currentPartitionFetchState.offset, topic, partitionId, newOffset))
                    } catch {
                      case e: Throwable =>
                        error("Error getting offset for partition [%s,%d] to broker %d".format(topic, partitionId, sourceBroker.id), e)
                        partitionsWithError += topicAndPartition
                    }
                  case _ =>
                    if (isRunning.get) {
                      error("Error for partition [%s,%d] to broker %d:%s".format(topic, partitionId, sourceBroker.id,
                        ErrorMapping.exceptionFor(partitionData.error).getClass))
                      partitionsWithError += topicAndPartition
                    }
                }
              })
        }
      }
    }

    if(partitionsWithError.size > 0) {
      info("handling partitions with error for %s".format(partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError)
    }

    if (newMsgSize < config.fetchMinBytes) {
      // the validBytes of FetchResponse can be zero, i.e. kafka brokers just try best to comply
      // minSize/maxWait set on FetchRequest. Therefore, it's better to check the threshold on our own
      debug("Get new bytes=%d smaller than threshold=%d, sleep awhileInMs=%d".format(newMsgSize, config.fetchMinBytes,
        config.fetchWaitMaxMs))
      Thread.sleep(config.fetchWaitMaxMs)
    } else {
      debug("Get new bytes=%d larger than threshold=%d".format(newMsgSize, config.fetchMinBytes))
    }
  }

  def addPartitions(partitionAndOffsets: Map[TopicAndPartition, Long]) {
    inLock(updateMapLock) {
      for ((topicAndPartition, offset) <- partitionAndOffsets) {
        // If the partitionMap already has the topic/partition, then do not update the map with the old offset
        if (!partitionAddMap.containsKey(topicAndPartition)) {
          partitionAddMap.put(
            topicAndPartition,
            if (kafka.consumer.PartitionTopicInfo.isOffsetInvalid(offset)) new PartitionFetchState(handleOffsetOutOfRange(topicAndPartition))
            else new PartitionFetchState(offset)
          )
        }
        if (partitionDeleteMap.containsKey(topicAndPartition)) {
          partitionDeleteMap.remove(topicAndPartition)
        }
      }
    }
  }

  def removePartitions(topicAndPartitions: Set[TopicAndPartition]) {
    info("enter removePartitions in CompactConsumerFetcherThread %d for set %s".format(this.getId, topicAndPartitions.toString()))
    inLock(updateMapLock) {
      topicAndPartitions.foreach { tp =>
        partitionDeleteMap.put(tp, true)
        if (partitionAddMap.containsKey(tp)) {
          partitionAddMap.remove(tp)
        }
      }
    }
    info("Finish removePartitions in CompactConsumerFetcherThread %d for set %s".format(this.getId, topicAndPartitions.toString()))
  }

}
