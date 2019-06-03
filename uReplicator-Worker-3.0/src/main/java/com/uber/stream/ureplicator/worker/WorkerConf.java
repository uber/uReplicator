/*
 * Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.ureplicator.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

public class WorkerConf extends PropertiesConfiguration {

  private static final String FEDERATED_ENABLED = "federated_enabled";
  private static final boolean DEFAULT_FEDERATED_ENABLE = false;

  private static final String HOSTNAME = "worker_hostname";

  private static final String CONSUMER_CONFIG_FILE = "consumer_config";
  private static final String DEFAULT_CONSUMER_CONFIG_FILE = "consumer.properties";

  private static final String PRODUCER_CONFIG_FILE = "producer_config";
  private static final String DEFAULT_PRODUCER_CONFIG_FILE = "producer.properties";

  private static final String HELIX_CONFIG_FILE = "helix_config";
  private static final String DEFAULT_HELIX_CONFIG_FILE = "helix.config";

  private static final String CLUSTER_CONFIG_FILE = "cluster_config";
  private static final String DEFAULT_CLUSTER_CONFIG_FILE = null;

  private static final String TOPIC_MAPPING_FILE = "topic_mappings";
  private static final String DEFAULT_TOPIC_MAPPING_FILE = null;

  private static final String ENABLE_TOPIC_PARTITION_COUNT_OBSERVER = "topic_partition_count_observer";
  private static final boolean DEFAULT_ENABLE_TOPIC_PARTITION_COUNT_OBSERVER = false;

  private static final String OFFSET_COMMIT_INTERVAL_MS = "offset_commit_interval_ms";
  private static final int DEFAULT_OFFSET_COMMIT_INTERVAL_MS = 60000;

  private static final String ABORT_ON_SEND_FAILURE = "abort_on_send_failure";
  private static final boolean DEFAULT_ABORT_ON_SEND_FAILURE = false;

  private static final String ENABLE_FILTER = "enable_filter";
  private static final boolean DEFAULT_ENABLE_FILTER = false;

  public WorkerConf() {
    super();
  }

  // TODO: abstract getProperty to to share with controller/manger conf class
  private String getProperty(String key, String defaultVal) {
    if (containsKey(key)) {
      return (String) getProperty(key);
    } else {
      return defaultVal;
    }
  }

  private Integer getProperty(String key, Integer defaultVal) {
    if (containsKey(key)) {
      return (Integer) getProperty(key);
    } else {
      return defaultVal;
    }
  }

  private boolean getProperty(String key, boolean defaultVal) {
    if (containsKey(key) && "true".equalsIgnoreCase(getProperty(key, ""))) {
      return true;
    } else if (containsKey(key) && "false".equalsIgnoreCase(getProperty(key, ""))) {
      return false;
    } else {
      return defaultVal;
    }
  }

  public boolean getFederatedEnabled() {
    return getProperty(FEDERATED_ENABLED, DEFAULT_FEDERATED_ENABLE);
  }

  public String getConsumerConfigFile() {
    return getProperty(CONSUMER_CONFIG_FILE, DEFAULT_CONSUMER_CONFIG_FILE);
  }

  public String getProducerConfigFile() {
    return getProperty(PRODUCER_CONFIG_FILE, DEFAULT_PRODUCER_CONFIG_FILE);
  }

  public String getHelixConfigFile() {
    return getProperty(HELIX_CONFIG_FILE, DEFAULT_HELIX_CONFIG_FILE);
  }

  public String getClusterConfigFile() {
    return getProperty(CLUSTER_CONFIG_FILE, DEFAULT_CLUSTER_CONFIG_FILE);
  }

  public String getTopicMappingFile() {
    return getProperty(TOPIC_MAPPING_FILE, DEFAULT_TOPIC_MAPPING_FILE);
  }

  public boolean enableDestinationPartitionCountObserver() {
    return getProperty(ENABLE_TOPIC_PARTITION_COUNT_OBSERVER,
        DEFAULT_ENABLE_TOPIC_PARTITION_COUNT_OBSERVER);

  }

  public Integer getOffsetCommitIntervalMs() {
    return getProperty(OFFSET_COMMIT_INTERVAL_MS, DEFAULT_OFFSET_COMMIT_INTERVAL_MS);

  }

  public boolean getAbortOnSendFailure() {
    return getProperty(ABORT_ON_SEND_FAILURE, DEFAULT_ABORT_ON_SEND_FAILURE);

  }

  public boolean getEnableFilter() {
    return getProperty(ENABLE_FILTER, DEFAULT_ENABLE_FILTER);
  }

  public String getHostname() {
    String defaultVal = "localhost";
    try {
      defaultVal = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
    }
    return getProperty(HOSTNAME, defaultVal);
  }


  public void setFederatedEnabled(boolean federatedEnabled) {
    setProperty(FEDERATED_ENABLED, String.valueOf(federatedEnabled));
  }

  public void setConsumerConfigFile(String consumerConfig) {
    setProperty(CONSUMER_CONFIG_FILE, consumerConfig);
  }

  public void setProducerConfigFile(String producerConfig) {
    setProperty(PRODUCER_CONFIG_FILE, producerConfig);
  }

  public void setHelixConfigFile(String helixConfig) {
    setProperty(HELIX_CONFIG_FILE, helixConfig);
  }

  public void setClusterConfigFile(String clusterConfig) {
    setProperty(CLUSTER_CONFIG_FILE, clusterConfig);
  }

  public void setTopicMappingFile(String topicMapping) {
    setProperty(TOPIC_MAPPING_FILE, topicMapping);
  }

  public void setEnableDestinationPartitionCountObserver(
      boolean enableTopicPartitionCountObserver) {
    setProperty(ENABLE_TOPIC_PARTITION_COUNT_OBSERVER,
        String.valueOf(enableTopicPartitionCountObserver));

  }

  public void setOffsetCommitIntervalMs(int offsetCommitIntervalMs) {
    setProperty(OFFSET_COMMIT_INTERVAL_MS, String.valueOf(offsetCommitIntervalMs));

  }

  public void setAbortOnSendFailure(boolean abortOnSendFailure) {
    setProperty(ABORT_ON_SEND_FAILURE, String.valueOf(abortOnSendFailure));

  }

  public void setEnableFilter(boolean enableFilter) {
    setProperty(ENABLE_FILTER, String.valueOf(enableFilter));
  }

  public void setHostname(String hostname) {
    setProperty(HOSTNAME, hostname);
  }

  public static Options constructWorkerOptions() {
    final Options workerOptions = new Options();
    workerOptions.addOption("help", false, "Help")
        .addOption(FEDERATED_ENABLED, true, "Whether to enable federated uReplicator")
        .addOption(CONSUMER_CONFIG_FILE, true,
            "Embedded consumer config for consuming from the source cluster.")
        .addOption(PRODUCER_CONFIG_FILE, true, "Embedded producer config.")
        .addOption(HELIX_CONFIG_FILE, true, "Embedded helix config.")
        .addOption(CLUSTER_CONFIG_FILE, true, "Embedded cluster config.")
        .addOption(TOPIC_MAPPING_FILE, true,
            "Path to file containing line deliminated mappings of topics to consume from and produce to.")
        .addOption(ENABLE_TOPIC_PARTITION_COUNT_OBSERVER, true,
            "Configure the uReplicator to observe destination topic partition count")
        .addOption(OFFSET_COMMIT_INTERVAL_MS, true, "Offset commit interval in ms")
        .addOption(ABORT_ON_SEND_FAILURE, true, "Configure the uReplicator exit on a failed send.")
        .addOption(ENABLE_FILTER, true,
            "Configure the uReplicator to filter message to send to dst cluster.")
        .addOption(HOSTNAME, true,
            "hostname for this host");
    return workerOptions;
  }

  public static WorkerConf getWorkerConf(CommandLine cmd) {
    WorkerConf workerConf = new WorkerConf();
    for (Option option : constructWorkerOptions().getOptions()) {
      String opt = option.getOpt();
      if (cmd.hasOption(opt)) {
        workerConf.setProperty(opt, cmd.getOptionValue(opt));
      }
    }
    return workerConf;
  }
}
