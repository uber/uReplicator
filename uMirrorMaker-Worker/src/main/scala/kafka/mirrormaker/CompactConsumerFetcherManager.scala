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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.client.ClientUtils
import kafka.cluster.{BrokerEndPoint, Cluster}
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerAndFetcherId, BrokerAndInitialOffset}
import kafka.utils.CoreUtils._
import kafka.utils.{ZkUtils, Logging, ShutdownableThread, SystemTime}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable.HashMap
import scala.collection.{JavaConversions, Map, Set, mutable}

/**
 * CompactConsumerFetcherManager uses LeaderFinderThread to handle the partition addition, deletion and leader change.
 * It will create fetcher threads at the max number of (num.consumer.fetchers) * (brokers).
 * Once CompactConsumerFetcherManager is created, startConnections() and stopAllConnections()
 * can be called repeatedly until shutdown() is called.
 *
 * @param consumerIdString
 * @param config
 * @param zkClient
 */
class CompactConsumerFetcherManager (private val consumerIdString: String,
                                     private val config: ConsumerConfig,
                                     private val zkClient : ZkClient)
  extends Logging with KafkaMetricsGroup {
  protected val name: String = "CompactConsumerFetcherManager-%d".format(SystemTime.milliseconds)
  private val clientId: String = config.clientId
  private val numFetchers: Int = config.numConsumerFetchers
  // map of (source broker_id, fetcher_id per source broker) => fetcher
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, CompactConsumerFetcherThread]
  private val mapLock = new Object
  this.logIdent = "[" + name + "] "

  private var partitionInfoMap = new ConcurrentHashMap[TopicAndPartition, PartitionTopicInfo]()
  private val partitionAddMap = new ConcurrentHashMap[TopicAndPartition, PartitionTopicInfo]
  private val partitionDeleteMap = new ConcurrentHashMap[TopicAndPartition, Boolean]
  private val partitionNewLeaderMap = new ConcurrentHashMap[TopicAndPartition, Boolean]
  private val updateMapLock = new ReentrantLock
  private var cluster: Cluster = null
  private val noLeaderPartitionSet = new mutable.HashSet[TopicAndPartition]
  private val lock = new ReentrantLock
  private var leaderFinderThread: ShutdownableThread = null
  private val correlationId = new AtomicInteger(0)

  newGauge("OwnedPartitionsCount",
    new Gauge[Int] {
      def value() = getPartitionInfoMapSize()
    },
    Map("clientId" -> clientId)
  )

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
  "MinFetchRate", {
    new Gauge[Double] {
      // current min fetch rate across all fetchers/topics/partitions
      def value = {
        val headRate: Double =
          fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
          fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
        })
      }
    }
  },
  Map("clientId" -> clientId)
  )

  private def getFetcherId(topic: String, partitionId: Int) : Int = {
    Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers
  }

  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): CompactConsumerFetcherThread = {
    new CompactConsumerFetcherThread(
      "CompactConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id),
      config, sourceBroker, partitionInfoMap, this)
  }

  def addFetcherForPartitions(partitionAndOffsets: Map[TopicAndPartition, BrokerAndInitialOffset]) {
    mapLock synchronized {
      debug("addFetcherForPartitions get lock")
      val partitionsPerFetcher = partitionAndOffsets.groupBy{ case(topicAndPartition, brokerAndInitialOffset) =>
        BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicAndPartition.topic, topicAndPartition.partition))}
      for ((brokerAndFetcherId, partitionAndOffsets) <- partitionsPerFetcher) {
        var fetcherThread: CompactConsumerFetcherThread = null
        fetcherThreadMap.get(brokerAndFetcherId) match {
          case Some(f) => fetcherThread = f
          case None =>
            fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
            fetcherThreadMap.put(brokerAndFetcherId, fetcherThread)
            fetcherThread.start
        }

        fetcherThreadMap(brokerAndFetcherId).addPartitions(partitionAndOffsets.map { case (topicAndPartition, brokerAndInitOffset) =>
          topicAndPartition -> brokerAndInitOffset.initOffset
        })
        info("Fetcher Thread for topic partitions: %s is %s".format(partitionAndOffsets.map{ case (topicAndPartition, brokerAndInitialOffset) =>
          "[" + topicAndPartition + ", InitialOffset " + brokerAndInitialOffset.initOffset + "] "}, fetcherThread.name))
      }
      debug("addFetcherForPartitions releasing lock")
    }

    info("Added fetcher for partitions %s".format(partitionAndOffsets.map{ case (topicAndPartition, brokerAndInitialOffset) =>
      "[" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] "}))
  }

  def removeFetcherForPartitions(partitions: Set[TopicAndPartition]) {
    debug("Enter removeFetcherForPartitions in CompactConsumerFetcherManager for set " + partitions.mkString(","))
    debug("Current thread size: " + fetcherThreadMap.size)
    mapLock synchronized {
      debug("removeFetcherForPartitions get lock")
      for ((key, fetcher) <- fetcherThreadMap) {
        fetcher.removePartitions(partitions)
      }
      debug("removeFetcherForPartitions releasing lock")
    }
    info("Removed fetcher for partitions %s".format(partitions.mkString(",")))
    debug("Finish removeFetcherForPartitions in CompactConsumerFetcherManager for set " + partitions.mkString(","))
  }

  def closeAllFetchers() {
    mapLock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
    }
  }

  def startConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster) {
    inLock(updateMapLock) {
      topicInfos.foreach(tpi => partitionAddMap.put(TopicAndPartition(tpi.topic, tpi.partitionId), tpi))
    }
    debug("Initializing partition(add) map to : " + partitionAddMap.keySet.toString())
    this.cluster = cluster
    leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread")
    leaderFinderThread.start()
  }

  /**
   * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
   * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
   * these partitions.
   */
  def stopConnections() {
    info("Stopping leader finder thread")
    if (leaderFinderThread != null) {
      leaderFinderThread.shutdown()
      leaderFinderThread = null
    }

    info("Stopping all fetchers")
    closeAllFetchers()

    // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
    partitionInfoMap = null
    noLeaderPartitionSet.clear()
    partitionAddMap.clear()
    partitionDeleteMap.clear()
    partitionNewLeaderMap.clear()

    info("All connections stopped")
  }

  def addPartitionsWithError(partitionList: Iterable[TopicAndPartition]) {
    debug("adding partitions with error %s".format(partitionList))
    if (partitionNewLeaderMap != null) {
      inLock(updateMapLock) {
        partitionList.foreach(p => partitionNewLeaderMap.put(p, true))
      }
    }
  }

  def addTopicPartition(pti: PartitionTopicInfo): Unit = {
    info("adding new topic-partition %s - %d".format(pti.topic, pti.partitionId))
    val topicAndPartition = TopicAndPartition(pti.topic, pti.partitionId)
    inLock(updateMapLock) {
      if (partitionAddMap != null) {
        partitionAddMap.put(topicAndPartition, pti)
      }
      if (partitionDeleteMap != null) {
        partitionDeleteMap.remove(topicAndPartition)
      }
    }
  }

  def removeTopicPartition(pti: PartitionTopicInfo): Unit = {
    info("Deleting topic-partition %s - %d".format(pti.topic, pti.partitionId))
    val topicAndPartition = TopicAndPartition(pti.topic, pti.partitionId)
    inLock(updateMapLock) {
      if (partitionDeleteMap != null) {
        partitionDeleteMap.put(topicAndPartition, true)
      }
      if (partitionAddMap != null) {
        partitionAddMap.remove(topicAndPartition)
      }
    }
    debug("Finish removeTopicPartition in CompactConsumerFetcherManager for topic-partition %s - %d".format(pti.topic, pti.partitionId))
  }

  def markTopicPartitionAsDeleted(tp: TopicAndPartition): Unit = {
    partitionInfoMap.get(tp).deleted.set(true)
  }

  def getPartitionInfoMapSize():Int = {
    var count = 0
    val piItr = partitionInfoMap.values().iterator()
    while (piItr.hasNext) {
      if (!piItr.next().deleted.get()) {
        count += 1
      }
    }
    count
  }

  private class LeaderFinderThread(name: String) extends ShutdownableThread(name) {
    def processAddDeleteSet() = {
      inLock(updateMapLock) {
        // add topic partition into partitionInfoMap
        val addIter = partitionAddMap.entrySet().iterator()
        while (addIter.hasNext) {
          val tpToAdd = addIter.next()
          partitionInfoMap.put(tpToAdd.getKey, tpToAdd.getValue)
          noLeaderPartitionSet.add(tpToAdd.getKey)
        }
        partitionAddMap.clear()

        // remove topic partition from partitionInfoMap
        if (partitionDeleteMap.size() > 0) {
          removeFetcherForPartitions(JavaConversions.asScalaSet(partitionDeleteMap.keySet()))
        }

        val deleteIter = partitionDeleteMap.entrySet().iterator()
        while (deleteIter.hasNext) {
          val tpToDelete = deleteIter.next()
          if (partitionInfoMap.containsKey(tpToDelete.getKey)) {
            markTopicPartitionAsDeleted(tpToDelete.getKey)
          }
          if (noLeaderPartitionSet.contains(tpToDelete.getKey)) {
            noLeaderPartitionSet.remove(tpToDelete.getKey)
          }
        }
        partitionDeleteMap.clear()

        // leader change
        val newLeaderIter = partitionNewLeaderMap.entrySet().iterator()
        while (newLeaderIter.hasNext) {
          val tpWithLeaderChange = newLeaderIter.next()
          noLeaderPartitionSet.add(tpWithLeaderChange.getKey)
        }
        partitionNewLeaderMap.clear()
      }
    }

    // thread responsible for adding the fetcher to the right broker when leader is available
    override def doWork() {
      val leaderForPartitionsMap = new HashMap[TopicAndPartition, BrokerEndPoint]
      lock.lock()
      try {
        processAddDeleteSet
        if (noLeaderPartitionSet.isEmpty) {
          trace("No partition for leader election.")
          // TODO: actual sleep time need to be tuned
          Thread.sleep(config.refreshLeaderBackoffMs)
          return
        }

        info("Partitions without leader %s".format(noLeaderPartitionSet))
        val brokers = ZkUtils.apply(zkClient, true).getAllBrokerEndPointsForChannel(SecurityProtocol.PLAINTEXT)

        val topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.map(m => m.topic).toSet,
          brokers,
          config.clientId,
          config.socketTimeoutMs,
          correlationId.getAndIncrement).topicsMetadata
        if(logger.isDebugEnabled) topicsMetadata.foreach(topicMetadata => debug(topicMetadata.toString()))
        topicsMetadata.foreach { tmd =>
          val topic = tmd.topic
          tmd.partitionsMetadata.foreach { pmd =>
            val topicAndPartition = TopicAndPartition(topic, pmd.partitionId)
            if(pmd.leader.isDefined && noLeaderPartitionSet.contains(topicAndPartition)) {
              info("Try find leader for topic: %s, partition:%d".format(topicAndPartition.topic, topicAndPartition.partition))
              val leaderBroker = pmd.leader.get
              leaderForPartitionsMap.put(topicAndPartition, leaderBroker)
              noLeaderPartitionSet -= topicAndPartition
            }
          }
        }
      } catch {
        case t: Throwable => {
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          else
            warn("Failed to find leader for %s".format(noLeaderPartitionSet), t)
        }
      } finally {
        lock.unlock()
      }

      try {
        addFetcherForPartitions(leaderForPartitionsMap.map{
          case (topicAndPartition, broker) =>
            topicAndPartition -> BrokerAndInitialOffset(broker, partitionInfoMap.get(topicAndPartition).getFetchOffset())}
        )
      } catch {
        case t: Throwable => {
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          else {
            warn("Failed to add leader for partitions %s; will retry".format(leaderForPartitionsMap.keySet.mkString(",")), t)
            lock.lock()
            noLeaderPartitionSet ++= leaderForPartitionsMap.keySet
            lock.unlock()
          }
        }
      }
      Thread.sleep(config.refreshLeaderBackoffMs)
    }
  }
}

