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
package com.uber.stream.kafka.mirrormaker.controller.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * AutoTopicWhitelistingManager will look at both source and destination Kafka brokers and pick
 * the topics in the intersection to add to the whitelist.
 */
public class AutoTopicWhitelistingManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AutoTopicWhitelistingManager.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final ScheduledExecutorService _executorService =
      Executors.newSingleThreadScheduledExecutor();

  private final int _refreshTimeInSec;
  private final int _initWaitTimeInSec;
  private TimeUnit _timeUnit = TimeUnit.SECONDS;
  // Metrics
  private final static String AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE =
      "AutoTopicWhitelistManager.%s";

  private final Counter _numWhitelistedTopics = new Counter();
  private final Counter _numAutoExpandedTopics = new Counter();
  private final Counter _numErrorTopics = new Counter();

  private final Counter _numOfAutoWhitelistingRuns = new Counter();
  private final KafkaBrokerTopicObserver _srcKafkaTopicObserver;
  private final KafkaBrokerTopicObserver _destKafkaTopicObserver;

  private final ZkClient _zkClient;
  private final String _blacklistedTopicsZPath;

  private final String _patternToExcludeTopics;
  private final Set<String> _blacklistedTopics =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public AutoTopicWhitelistingManager(KafkaBrokerTopicObserver srcKafkaTopicObserver,
      KafkaBrokerTopicObserver destKafkaTopicObserver,
      HelixMirrorMakerManager helixMirrorMakerManager,
      String patternToExcludeTopics) {
    this(srcKafkaTopicObserver, destKafkaTopicObserver, helixMirrorMakerManager,
        patternToExcludeTopics, 60 * 10);
  }

  public AutoTopicWhitelistingManager(KafkaBrokerTopicObserver srcKafkaTopicObserver,
      KafkaBrokerTopicObserver destKafkaTopicObserver,
      HelixMirrorMakerManager helixMirrorMakerManager,
      String patternToExcludeTopics,
      int refreshTimeInSec) {
    this(srcKafkaTopicObserver, destKafkaTopicObserver, helixMirrorMakerManager,
            patternToExcludeTopics, refreshTimeInSec, 120);
  }

  public AutoTopicWhitelistingManager(KafkaBrokerTopicObserver srcKafkaTopicObserver,
                                      KafkaBrokerTopicObserver destKafkaTopicObserver,
                                      HelixMirrorMakerManager helixMirrorMakerManager,
                                      String patternToExcludeTopics,
                                      int refreshTimeInSec,
                                      int initWaitTimeInSec) {
    _srcKafkaTopicObserver = srcKafkaTopicObserver;
    _destKafkaTopicObserver = destKafkaTopicObserver;
    _helixMirrorMakerManager = helixMirrorMakerManager;
    _patternToExcludeTopics = patternToExcludeTopics;
    _refreshTimeInSec = refreshTimeInSec;
    _initWaitTimeInSec = initWaitTimeInSec;
    _zkClient = new ZkClient(_helixMirrorMakerManager.getHelixZkURL(), 30000, 30000,
            ZKStringSerializer$.MODULE$);
    _blacklistedTopicsZPath =
            String.format("/%s/BLACKLISTED_TOPICS", _helixMirrorMakerManager.getHelixClusterName());
  }

  public void start() {
    registerMetrics();

    LOGGER.info("Creating zkpath={} for blacklisted topics", _blacklistedTopicsZPath);
    maybeCreateZkPath(_blacklistedTopicsZPath);

    // Report current status every one minutes.
    LOGGER.info("Trying to schedule auto topic whitelisting job at rate {} {} !", _refreshTimeInSec,
        _timeUnit.toString());
    _executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        if (_helixMirrorMakerManager.isLeader()) {
          _numOfAutoWhitelistingRuns.inc();
          LOGGER.info("Trying to run topic whitelisting job");
          Set<String> candidateTopicsToWhitelist = null;
          try {
            candidateTopicsToWhitelist = getCandidateTopicsToWhitelist();
          } catch (Exception e) {
            LOGGER.error("Failed to get candidate topics: ", e);
            candidateTopicsToWhitelist = new HashSet<String>();
          }
          LOGGER.info("Trying to whitelist topics: {}",
              Arrays.toString(candidateTopicsToWhitelist.toArray(new String[0])));
          _numErrorTopics.dec(_numErrorTopics.getCount());
          whitelistCandiateTopics(candidateTopicsToWhitelist);
        } else {
          LOGGER.debug("Not leader, skip auto topic whitelisting!");
        }

      }
    }, Math.min(_initWaitTimeInSec, _refreshTimeInSec), _refreshTimeInSec, _timeUnit);
  }

  private void maybeCreateZkPath(String path) {
    try {
      _zkClient.createPersistent(path, true);
    } catch (ZkNodeExistsException e) {
      LOGGER.debug("Path={} is created in zk already", path);
    }
  }

  private Set<String> getCandidateTopicsToWhitelist() {
    Set<String> candidateTopics = new HashSet<String>(_srcKafkaTopicObserver.getAllTopics());
    candidateTopics.retainAll(_destKafkaTopicObserver.getAllTopics());
    candidateTopics.removeAll(_helixMirrorMakerManager.getTopicLists());
    candidateTopics.addAll(getPartitionMismatchedTopics());

    loadBlacklistedTopics();
    LOGGER.info("BlacklistedTopics={} and ExcludingPattern={}", _blacklistedTopics,
        _patternToExcludeTopics);

    Iterator<String> itr = candidateTopics.iterator();
    while (itr.hasNext()) {
      String topic = itr.next();
      if (_blacklistedTopics.contains(topic)) {
        LOGGER.info("Exclude topic={} by blacklist", topic);
        itr.remove();
      } else if (topic.matches(_patternToExcludeTopics)) {
        LOGGER.info("Exclude topic={} by pattern", topic);
        itr.remove();
      }
    }

    return candidateTopics;
  }

  private Set<String> getPartitionMismatchedTopics() {
    Set<String> partitionsMismatchedTopics = new HashSet<String>();
    for (String topicName : _helixMirrorMakerManager.getTopicLists()) {
      int numPartitionsInHelix =
          _helixMirrorMakerManager.getIdealStateForTopic(topicName).getNumPartitions();
      if (_srcKafkaTopicObserver.getTopicPartition(topicName) != null) {
        int numPartitionsInSrcBroker =
            _srcKafkaTopicObserver.getTopicPartition(topicName).getPartition();
        if (numPartitionsInHelix != numPartitionsInSrcBroker) {
          partitionsMismatchedTopics.add(topicName);
        }
      }
    }
    return partitionsMismatchedTopics;
  }

  private void loadBlacklistedTopics() {
    _blacklistedTopics.clear();
    _blacklistedTopics.addAll(_zkClient.getChildren(_blacklistedTopicsZPath));
  }

  private void whitelistCandiateTopics(Set<String> candidateTopicsToWhitelist) {
    for (String topic : candidateTopicsToWhitelist) {
      TopicPartition tp = _srcKafkaTopicObserver.getTopicPartition(topic);
      if (tp == null) {
        LOGGER.error("Shouldn't hit here, don't know why topic {} is not in src Kafka cluster",
            topic);
        _numErrorTopics.inc();
      } else {
        if (_helixMirrorMakerManager.isTopicExisted(topic)) {
          LOGGER.info("Trying to expand topic: {} with {} partitions", tp.getTopic(),
              tp.getPartition());
          _helixMirrorMakerManager.expandTopicInMirrorMaker(tp);
          _numAutoExpandedTopics.inc();
        } else {
          LOGGER.info("Trying to whitelist topic: {} with {} partitions", tp.getTopic(),
              tp.getPartition());
          _helixMirrorMakerManager.addTopicToMirrorMaker(tp);
          _numWhitelistedTopics.inc();
        }
      }
    }
  }

  private void registerMetrics() {
    try {
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numOfAutoWhitelistingRuns"),
          _numOfAutoWhitelistingRuns);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numWhitelistedTopics"),
          _numWhitelistedTopics);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numAutoExpandedTopics"),
          _numAutoExpandedTopics);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric(
          String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numErrorTopics"),
          _numErrorTopics);
    } catch (Exception e) {
      LOGGER.error("Got exception during register metrics");
    }
  }

  public void addIntoBlacklist(String topic) {
    maybeCreateZkPath(_blacklistedTopicsZPath + "/" + topic);
    LOGGER.info("topic={} is added to blacklist on zk", topic);
  }

  public void removeFromBlacklist(String topic) {
    ZkUtils.deletePath(_zkClient, _blacklistedTopicsZPath + "/" + topic);
    LOGGER.info("topic={} is removed from blacklist on zk", topic);
  }

}
