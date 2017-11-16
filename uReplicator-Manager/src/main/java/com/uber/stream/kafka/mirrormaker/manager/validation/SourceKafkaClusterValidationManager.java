/*
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
package com.uber.stream.kafka.mirrormaker.manager.validation;

import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validate idealstates and source kafka cluster info and update related metrics.
 */
public class SourceKafkaClusterValidationManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceKafkaClusterValidationManager.class);
  private static final String CONFIG_KAFKA_CLUSTER_KEY_PREFIX = "kafka.cluster.zkStr.";

  private final ManagerConf _conf;
  private final Map<String, KafkaBrokerTopicObserver> _clusterToObserverMap;


  public SourceKafkaClusterValidationManager(ManagerConf conf) {
    _conf = conf;
    _clusterToObserverMap = new HashMap<>();
    for (String cluster : conf.getSourceClusters()) {
      String srcKafkaZkPath = (String) conf.getProperty(CONFIG_KAFKA_CLUSTER_KEY_PREFIX + cluster);
      _clusterToObserverMap.put(cluster, new KafkaBrokerTopicObserver(cluster, srcKafkaZkPath));
    }
  }

  public void start() {
    LOGGER.info("Register KafkaBrokerTopicObserver");
  }

  public void stop() {
    LOGGER.info("Stop KafkaBrokerTopicObserver");
    for (KafkaBrokerTopicObserver observer : _clusterToObserverMap.values()) {
      observer.stop();
    }
  }

  public Map<String, KafkaBrokerTopicObserver> getClusterToObserverMap() {
    return _clusterToObserverMap;
  }

}
