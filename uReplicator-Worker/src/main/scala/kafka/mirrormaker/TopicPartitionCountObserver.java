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
package kafka.mirrormaker;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * Get partition number for topics from zookeeper
 * Created by hongliang on 1/16/18.
 */
public class TopicPartitionCountObserver implements IZkChildListener {
  private static final Logger logger = LoggerFactory.getLogger(TopicPartitionCountObserver.class);

  private final ScheduledExecutorService cronExecutor;

  private final String zkConnect;
  private final ZkClient zkClient;
  private final String zkPath;

  private final int refreshIntervalMs;

  private final ConcurrentHashMap<String, Integer> topicPartitionMap = new ConcurrentHashMap<String, Integer>();

  public TopicPartitionCountObserver(final String zkConnect, final String zkPath,
      final int zkConnectionTimeoutMs, final int zkSessionTimeoutMs, final int refreshIntervalMs) {
    logger.info("Init TopicPartitionCountObserver for zkConnect={} and zkPath={}", zkConnect, zkPath);
    this.zkConnect = zkConnect;
    this.zkPath = zkPath;

    zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs,
        ZKStringSerializer$.MODULE$);

    this.refreshIntervalMs = refreshIntervalMs;
    cronExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("topic-observer-%d").build());
  }

  public void start() {
    logger.info("Start TopicPartitionCountObserver for zkConnect={} and zkPath={}", zkConnect, zkPath);

    zkClient.subscribeChildChanges(zkPath, this);

    cronExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          logger.info("Update topics info periodically for zkPath={}", zkPath);
          updateTopicPartitionInfoMap(topicPartitionMap.keySet());
        } catch (Exception e) {
          logger.warn("Failed to fresh topics info for zkPath={} periodically", zkPath, e);
        }
      }
    }, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    logger.info("Shutdown TopicPartitionCountObserver for zkPath={}", zkPath);
    zkClient.unsubscribeChildChanges(zkPath, this);
    zkClient.close();
    cronExecutor.shutdownNow();
  }

  public int getPartitionCount(String topicName) {
    if (topicPartitionMap != null && topicPartitionMap.containsKey(topicName)) {
      return topicPartitionMap.get(topicName);
    } else {
      logger.error("getPartitionCount for topicName: {} failed", topicName);
      return 0;
    }

  }

  public void addTopic(String topicName) {
    logger.info("Add topic={} to check partition count", topicName);
    topicPartitionMap.putIfAbsent(topicName, 0);
    updateTopicPartitionInfoMap(Collections.singleton(topicName));
  }

  /**
   * deleteTopic() is not safe because msg can be left in memory for consumption even after topic
   * is removed. And the cost to leave unused topics in the map should be negligible since the
   * number of total topics is not large as for now.
   */

  @Override
  public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
    final Set<String> topicsFromZookeeper = new HashSet<String>(currentChildren);
    logger.info("Update topics info by zk event for zkPath={}", zkPath);
    topicsFromZookeeper.retainAll(topicPartitionMap.keySet());
    updateTopicPartitionInfoMap(topicsFromZookeeper);
  }

  private void updateTopicPartitionInfoMap(final Set<String> topicsToCheck) {
    if (topicsToCheck.size() > 0) {
      // get topic partition count and maybe update partition counts for existing topics
      scala.collection.mutable.Map<String, scala.collection.Map<Object, Seq<Object>>> partitionAssignmentForTopics =
          ZkUtils.getPartitionAssignmentForTopics(zkClient, JavaConversions.asScalaBuffer(ImmutableList.copyOf(topicsToCheck)));

      for (String topic : topicsToCheck) {
        try {
          topicPartitionMap.put(topic, partitionAssignmentForTopics.get(topic).get().size());
        } catch (Exception e) {
          logger.warn("Failed to get topicPartition info for topic={} of zkPath={}",
              topic, zkPath, e);
        }
      }
    }
  }
}
