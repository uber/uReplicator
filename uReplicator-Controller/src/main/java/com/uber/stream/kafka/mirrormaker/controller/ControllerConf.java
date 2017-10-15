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
package com.uber.stream.kafka.mirrormaker.controller;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Controller configs:
 * Helix configs, Controller Rest layer and reporting.
 *
 * @author xiangfu
 */
public class ControllerConf extends PropertiesConfiguration {

  private static final String CONTROLLER_PORT = "controller.port";
  private static final String ZK_STR = "controller.zk.str";
  private static final String HELIX_CLUSTER_NAME = "controller.helix.cluster.name";
  private static final String INSTANCE_ID = "controller.instance.id";
  private static final String ENV = "controller.environment";
  private static final String CONTROLLER_MODE = "controller.mode";

  private static final String GRAPHITE_HOST = "controller.graphite.host";
  private static final String GRAPHITE_PORT = "controller.graphite.port";

  private static final String C3_HOST = "controller.c3.host";
  private static final String DEFAULT_C3_HOST = "localhost";

  private static final String C3_PORT = "controller.c3.port";
  private static final int DEFAULT_C3_PORT = 0;

  private static final String SRC_KAFKA_ZK_PATH = "controller.srckafka.zkStr";
  private static final String DEST_KAFKA_ZK_PATH = "controller.destkafka.zkStr";
  private static final String CONSUMER_COMMIT_ZK_PATH = "controller.consumerCommit.zkStr";
  private static final String ENABLE_AUTO_WHITELIST = "controller.enable.auto.whitelist";
  private static final String ENABLE_AUTO_TOPIC_EXPANSION =
      "controller.enable.auto.topic.expansion";
  private static final String ENABLE_SRC_KAFKA_VALIDATION =
      "controller.enable.src.kafka.validation";

  private static final String NUM_OFFSET_THREAD = "controller.num.offset.thread";
  private static final int DEFAULT_NUM_OFFSET_THREAD = 10;

  private static final String OFFSET_REFRESH_INTERVAL_IN_SEC = "controller.offset.refresh.interval.in.sec";
  private static final int DEFAULT_OFFSET_REFRESH_INTERVAL_IN_SEC = 300;

  private static final String GROUP_ID = "controller.group.id";

  private static final String BACKUP_TO_GIT = "controller.backup.to.git";
  private static final String REMOTE_BACKUP_REPO = "controller.remote.backup.git";
  private static final String LOCAL_GIT_REPO = "controller.local.git.repo";
  private static final String LOCAL_FILE_BACKUP = "controller.local.backup.file.path";

  private static final String PATTERN_TO_EXCLUDE_TOPICS = "controller.pattern.exclude.topics";

  private static final String AUTO_REBALANCE_DELAY_IN_SECONDS =
      "controller.auto.rebalance.delay.in.seconds";

  private static final int DEFAULT_AUTO_REBALANCE_DELAY_IN_SECONDS = 120;

  private static final String WHITELIST_REFRESH_TIME_IN_SECONDS = "controller.refresh.time.in.seconds";

  private static final int DEFAULT_WHITELIST_REFRESH_TIME_IN_SECONDS = 600;

  private static final String INIT_WAIT_TIME_IN_SECONDS = "controller.init.wait.time.in.seconds";

  private static final int DEFAULT_INIT_WAIT_TIME_IN_SECONDS = 120;

  private static final String AUTO_REBALANCE_PERIOD_IN_SECONDS =
      "controller.auto.rebalance.period.in.seconds";

  private static final String AUTO_REBALANCE_MIN_INTERVAL_IN_SECONDS =
      "controller.auto.rebalance.min.interval.in.seconds";

  private static final int DEFAULT_AUTO_REBALANCE_MIN_INTERVAL_IN_SECONDS = 600;

  private static final String AUTO_REBALANCE_WORKLOAD_RATIO_THRESHOLD =
      "controller.auto.rebalance.workload.ratio.threshold";

  private static final double DEFAULT_AUTO_REBALANCE_WORKLOAD_RATIO_THRESHOLD = 1.2;

  private static final String WORKLOAD_REFRESH_PERIOD_IN_SECONDS =
      "controller.workload.refresh.period.in.seconds";

  private static final int DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS = 600;

  private static final String MAX_DEDICATED_LAGGING_INSTANCES_RATIO = "controller.auto.rebalance.max.dedicated.ratio";

  private static final double DEFAULT_MAX_DEDICATED_LAGGING_INSTANCES_RATIO = 0.5;

  private static final String defaultLocal = "/var/log/kafka-mirror-maker-controller";

  public ControllerConf() {
    super();
    this.setDelimiterParsingDisabled(true);
  }

  public void setPatternToExcludeTopics(String pattern) {
    setProperty(PATTERN_TO_EXCLUDE_TOPICS, pattern);
  }

  public void setHelixClusterName(String clusterName) {
    setProperty(HELIX_CLUSTER_NAME, clusterName);
  }

  public void setControllerPort(String port) {
    setProperty(CONTROLLER_PORT, port);
  }

  public void setControllerMode(String mode) {
    setProperty(CONTROLLER_MODE, mode);
  }

  public void setZkStr(String zkStr) {
    setProperty(ZK_STR, zkStr);
  }

  public void setInstanceId(String instanceId) {
    setProperty(INSTANCE_ID, instanceId);
  }

  public void setEnvironment(String environment) {
    setProperty(ENV, environment);
  }

  public void setGraphiteHost(String graphiteHost) {
    setProperty(GRAPHITE_HOST, graphiteHost);
  }

  public void setGraphitePort(String graphitePort) {
    setProperty(GRAPHITE_PORT, Integer.valueOf(graphitePort));
  }

  public void setC3Host(String C3Host) {
    setProperty(C3_HOST, C3Host);
  }

  public void setC3Port(String C3Port) {
    setProperty(C3_PORT, Integer.valueOf(C3Port));
  }

  public void setSrcKafkaZkPath(String srcKafkaZkPath) {
    setProperty(SRC_KAFKA_ZK_PATH, srcKafkaZkPath);
  }

  public void setDestKafkaZkPath(String destKafkaZkPath) {
    setProperty(DEST_KAFKA_ZK_PATH, destKafkaZkPath);
  }

  public void setConsumerCommitZkPath(String consumerCommitZkPath) {
    setProperty(CONSUMER_COMMIT_ZK_PATH, consumerCommitZkPath);
  }

  public void setEnableAutoWhitelist(String enableAutoWhitelist) {
    setProperty(ENABLE_AUTO_WHITELIST, enableAutoWhitelist);
  }

  public void setEnableAutoTopicExpansion(String enableAutoTopicExpansion) {
    setProperty(ENABLE_AUTO_TOPIC_EXPANSION, enableAutoTopicExpansion);
  }

  public void setEnableSrcKafkaValidation(String enableSrcKafkaValidation) {
    setProperty(ENABLE_SRC_KAFKA_VALIDATION, enableSrcKafkaValidation);
  }

  public void setNumOffsetThread(String numOffsetThread) {
    setProperty(NUM_OFFSET_THREAD, Integer.valueOf(numOffsetThread));
  }

  public void setOffsetRefreshIntervalInSec(String offsetRefreshIntervalInSec) {
    setProperty(OFFSET_REFRESH_INTERVAL_IN_SEC, Integer.valueOf(offsetRefreshIntervalInSec));
  }

  public void setGroupId(String groupId) {
    setProperty(GROUP_ID, groupId);
  }

  public void setBackUpToGit(String backUpOption) {
    setProperty(BACKUP_TO_GIT, Boolean.valueOf(backUpOption));
  }

  public void setRemoteBackupRepo(String remoteBackupRepo) {
    setProperty(REMOTE_BACKUP_REPO, remoteBackupRepo);
  }

  public void setLocalGitRepoPath(String localGitRepoPath) {
    setProperty(LOCAL_GIT_REPO, localGitRepoPath);
  }

  public void setLocalBackupFilePath(String localBackupFilePath) {
    setProperty(LOCAL_FILE_BACKUP, localBackupFilePath);
  }

  public void setAutoRebalanceDelayInSeconds(String autoRebalanceDelayInSeconds) {
    setProperty(AUTO_REBALANCE_DELAY_IN_SECONDS, Integer.parseInt(autoRebalanceDelayInSeconds));
  }

  public void setWhitelistRefreshTimeInSeconds(String refreshTimeInSeconds) {
    setProperty(WHITELIST_REFRESH_TIME_IN_SECONDS, Integer.parseInt(refreshTimeInSeconds));
  }

  public void setInitWaitTimeInSeconds(String initWaitTimeInSeconds) {
    setProperty(INIT_WAIT_TIME_IN_SECONDS, Integer.parseInt(initWaitTimeInSeconds));
  }

  public void setAutoRebalancePeriodInSeconds(String autoRebalancePeriodInSeconds) {
    setProperty(AUTO_REBALANCE_PERIOD_IN_SECONDS, Integer.parseInt(autoRebalancePeriodInSeconds));
  }

  public void setAutoRebalanceMinIntervalInSeconds(String autoRebalanceMinIntervalInSeconds) {
    setProperty(AUTO_REBALANCE_MIN_INTERVAL_IN_SECONDS, Integer.parseInt(autoRebalanceMinIntervalInSeconds));
  }

  public void setAutoRebalanceWorkloadRatioThreshold(String autoRebalanceWorkloadRatioThreshold) {
    setProperty(AUTO_REBALANCE_WORKLOAD_RATIO_THRESHOLD, Double.parseDouble(autoRebalanceWorkloadRatioThreshold));
  }

  public void setWorkloadRefreshPeriodInSeconds(String workloadRefreshPeriodInSeconds) {
    setProperty(WORKLOAD_REFRESH_PERIOD_IN_SECONDS, Integer.parseInt(workloadRefreshPeriodInSeconds));
  }

  public void setMaxDedicatedLaggingInstancesRatio(String maxDedicatedLaggingInstancesRatio) {
    setProperty(MAX_DEDICATED_LAGGING_INSTANCES_RATIO, Double.parseDouble(maxDedicatedLaggingInstancesRatio));
  }

  public String getPatternToExcludeTopics() {
    return (String) getProperty(PATTERN_TO_EXCLUDE_TOPICS);
  }

  public String getRemoteBackupRepo() {
    return (String) getProperty(REMOTE_BACKUP_REPO);
  }

  public String getLocalGitRepoPath() {
    return (String) getProperty(LOCAL_GIT_REPO);
  }

  public String getLocalBackupFilePath() {
    return (String) getProperty(LOCAL_FILE_BACKUP);
  }

  public String getHelixClusterName() {
    return (String) getProperty(HELIX_CLUSTER_NAME);
  }

  public String getControllerPort() {
    return (String) getProperty(CONTROLLER_PORT);
  }

  public String getControllerMode() {
    return (String) getProperty(CONTROLLER_MODE);
  }

  public String getZkStr() {
    return (String) getProperty(ZK_STR);
  }

  public String getInstanceId() {
    if (!containsKey(INSTANCE_ID)) {
      try {
        setInstanceId(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        // Do nothing
      }
    }
    return (String) getProperty(INSTANCE_ID);
  }

  public String getEnvironment() {
    return (String) getProperty(ENV);
  }

  public String getGraphiteHost() {
    return (String) getProperty(GRAPHITE_HOST);
  }

  public Integer getGraphitePort() {
    return (Integer) getProperty(GRAPHITE_PORT);
  }

  public String getC3Host() {
    if (containsKey(C3_HOST)) {
      return (String) getProperty(C3_HOST);
    } else {
      return DEFAULT_C3_HOST;
    }
  }

  public Integer getC3Port() {
    if (containsKey(C3_PORT)) {
      return (Integer) getProperty(C3_PORT);
    } else {
      return DEFAULT_C3_PORT;
    }
  }

  public String getSrcKafkaZkPath() {
    return (String) getProperty(SRC_KAFKA_ZK_PATH);
  }

  public String getDestKafkaZkPath() {
    return (String) getProperty(DEST_KAFKA_ZK_PATH);
  }

  public String getConsumerCommitZkPath() {
    if (containsKey(CONSUMER_COMMIT_ZK_PATH)) {
      return (String) getProperty(CONSUMER_COMMIT_ZK_PATH);
    }
    return "";
  }

  public Boolean getBackUpToGit() {
    return (Boolean) getProperty(BACKUP_TO_GIT);
  }

  public Integer getAutoRebalanceDelayInSeconds() {
    if (containsKey(AUTO_REBALANCE_DELAY_IN_SECONDS)) {
      return (Integer) getProperty(AUTO_REBALANCE_DELAY_IN_SECONDS);
    } else {
      return DEFAULT_AUTO_REBALANCE_DELAY_IN_SECONDS;
    }
  }

  public Integer getInitWaitTimeInSeconds() {
    if (containsKey(INIT_WAIT_TIME_IN_SECONDS)) {
      return (Integer) getProperty(INIT_WAIT_TIME_IN_SECONDS);
    } else {
      return DEFAULT_INIT_WAIT_TIME_IN_SECONDS;
    }
  }

  public Integer getWhitelistRefreshTimeInSeconds() {
    if (containsKey(WHITELIST_REFRESH_TIME_IN_SECONDS)) {
      return (Integer) getProperty(WHITELIST_REFRESH_TIME_IN_SECONDS);
    } else {
      return DEFAULT_WHITELIST_REFRESH_TIME_IN_SECONDS;
    }
  }

  public Integer getAutoRebalancePeriodInSeconds() {
    if (containsKey(AUTO_REBALANCE_PERIOD_IN_SECONDS)) {
      return (Integer) getProperty(AUTO_REBALANCE_PERIOD_IN_SECONDS);
    } else {
      return 0;
    }
  }

  public Integer getAutoRebalanceMinIntervalInSeconds() {
    if (containsKey(AUTO_REBALANCE_MIN_INTERVAL_IN_SECONDS)) {
      return (Integer) getProperty(AUTO_REBALANCE_MIN_INTERVAL_IN_SECONDS);
    } else {
      return DEFAULT_AUTO_REBALANCE_MIN_INTERVAL_IN_SECONDS;
    }
  }

  public Double getAutoRebalanceWorkloadRatioThreshold() {
    if (containsKey(AUTO_REBALANCE_WORKLOAD_RATIO_THRESHOLD)) {
      return (Double) getProperty(AUTO_REBALANCE_WORKLOAD_RATIO_THRESHOLD);
    } else {
      return DEFAULT_AUTO_REBALANCE_WORKLOAD_RATIO_THRESHOLD;
    }
  }

  public Integer getWorkloadRefreshPeriodInSeconds() {
    if (containsKey(WORKLOAD_REFRESH_PERIOD_IN_SECONDS)) {
      return (Integer) getProperty(WORKLOAD_REFRESH_PERIOD_IN_SECONDS);
    } else {
      return DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS;
    }
  }

  public Double getMaxDedicatedLaggingInstancesRatio() {
    if (containsKey(MAX_DEDICATED_LAGGING_INSTANCES_RATIO)) {
      return (Double) getProperty(MAX_DEDICATED_LAGGING_INSTANCES_RATIO);
    } else {
      return DEFAULT_MAX_DEDICATED_LAGGING_INSTANCES_RATIO;
    }
  }

  public boolean getEnableAutoWhitelist() {
    if (containsKey(ENABLE_AUTO_WHITELIST)) {
      try {
        return "true".equalsIgnoreCase((String) getProperty(ENABLE_AUTO_WHITELIST));
      } catch (Exception e) {
        return false;
      }
    }
    return false;
  }

  public boolean getEnableAutoTopicExpansion() {
    if (containsKey(ENABLE_AUTO_TOPIC_EXPANSION)) {
      try {
        return "true".equalsIgnoreCase((String) getProperty(ENABLE_AUTO_TOPIC_EXPANSION));
      } catch (Exception e) {
        return false;
      }
    }
    return false;
  }

  public boolean getEnableSrcKafkaValidation() {
    if (containsKey(ENABLE_SRC_KAFKA_VALIDATION)) {
      try {
        return "true".equalsIgnoreCase((String) getProperty(ENABLE_SRC_KAFKA_VALIDATION));
      } catch (Exception e) {
        return false;
      }
    }
    return false;
  }

  public Integer getNumOffsetThread() {
    if (containsKey(NUM_OFFSET_THREAD)) {
      return (Integer) getProperty(NUM_OFFSET_THREAD);
    }
    return DEFAULT_NUM_OFFSET_THREAD;
  }

  public Integer getOffsetRefreshIntervalInSec() {
    if (containsKey(OFFSET_REFRESH_INTERVAL_IN_SEC)) {
      return (Integer) getProperty(OFFSET_REFRESH_INTERVAL_IN_SEC);
    }
    return DEFAULT_OFFSET_REFRESH_INTERVAL_IN_SEC;
  }

  public String getGroupId() {
    if (containsKey(GROUP_ID)) {
      return (String) getProperty(GROUP_ID);
    }
    return "";
  }

  @SuppressWarnings("rawtypes")
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator keysIter = getKeys();
    sb.append("\n{\n");
    while (keysIter.hasNext()) {
      Object key = keysIter.next();
      Object value = getProperty(key + "");
      sb.append("\t" + key + " : " + value + "\n");
    }
    sb.append("}\n");
    return sb.toString();
  }

  public static Options constructControllerOptions() {
    final Options controllerOptions = new Options();
    controllerOptions.addOption("help", false, "Help")
        .addOption("example1", false, "Start with default example")
        .addOption("example2", false, "Start with autowhitelisting example")
        .addOption("helixClusterName", true, "Helix Cluster Name")
        .addOption("mode", true, "Controller Mode")
        .addOption("zookeeper", true, "Zookeeper path")
        .addOption("port", true, "Controller port number")
        .addOption("env", true, "Environment")
        .addOption("instanceId", true, "InstanceId")
        .addOption("graphiteHost", true, "Graphite Host")
        .addOption("graphitePort", true, "Graphite Port")
        .addOption("c3Host", true, "Chaperone3 Host")
        .addOption("c3Port", true, "Chaperone3 Port")
        .addOption("enableAutoWhitelist", true, "Enable Auto Whitelist")
        .addOption("enableAutoTopicExpansion", true, "Enable Auto Topic Expansion during Source Kafka Validation")
        .addOption("patternToExcludeTopics", true, "Exclude specific topics by pattern")
        .addOption("enableSrcKafkaValidation", true, "Enable Source Kafka Validation")
        .addOption("srcKafkaZkPath", true, "Source Kafka Zookeeper Path")
        .addOption("destKafkaZkPath", true, "Destination Kafka Zookeeper Path")
        .addOption("consumerCommitZkPath", true, "Consumer commit Zookeeper Path")
        .addOption("autoRebalanceDelayInSeconds", true, "Auto Rebalance Delay in seconds")
        .addOption("refreshTimeInSeconds", true, "Controller Whitelist Manager Refresh Time in seconds")
        .addOption("initWaitTimeInSeconds", true, "Controller Init Delay in seconds")
        .addOption("autoRebalancePeriodInSeconds", true, "Period to try auto rebalancing in seconds")
        .addOption("autoRebalanceMinIntervalInSeconds", true, "Minimum interval between auto rebalancing in seconds")
        .addOption("workloadRefreshPeriodInSeconds", true, "The period to refresh workload information in seconds")
        .addOption("autoRebalanceWorkloadRatioThreshold", true,
            "The ratio of workload compared to average for auto workload rebalance")
        .addOption("maxDedicatedLaggingInstancesRatio", true,
            "The ratio of instances dedicated for serving lagging partitions")
        .addOption("numOffsetThread", true, "Number of threads to fetch topic offsets")
        .addOption("offsetRefreshIntervalInSec", true, "Topic offset monitor refresh interval")
        .addOption("groupId", true, "Consumer group id")
        .addOption("backUpToGit", true, "Backup controller metadata to git (true) or local file (false)")
        .addOption("remoteBackupRepo", true, "Remote Backup Repo to store cluster state")
        .addOption("localGitRepoClonePath", true, "Clone location of the remote git backup repo")
        .addOption("localBackupFilePath", true, "Local backup file location");
    return controllerOptions;
  }

  public static ControllerConf getControllerConf(CommandLine cmd) {
    ControllerConf controllerConf = new ControllerConf();
    if (cmd.hasOption("helixClusterName")) {
      controllerConf.setHelixClusterName(cmd.getOptionValue("helixClusterName"));
    } else {
      throw new RuntimeException("Missing option: --helixClusterName");
    }
    if (cmd.hasOption("zookeeper")) {
      controllerConf.setZkStr(cmd.getOptionValue("zookeeper"));
    } else {
      throw new RuntimeException("Missing option: --zookeeper");
    }
    if (cmd.hasOption("port")) {
      controllerConf.setControllerPort(cmd.getOptionValue("port"));
    } else {
      throw new RuntimeException("Missing option: --port");
    }
    if (cmd.hasOption("mode")) {
      controllerConf.setControllerMode(cmd.getOptionValue("mode"));
    } else {
      controllerConf.setControllerMode("auto");
    }
    if (cmd.hasOption("instanceId")) {
      controllerConf.setInstanceId(cmd.getOptionValue("instanceId"));
    } else {
      try {
        controllerConf.setInstanceId(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        // Do nothing
      }
    }
    if (cmd.hasOption("env")) {
      controllerConf.setEnvironment(cmd.getOptionValue("env"));
    } else {
      controllerConf.setEnvironment("env");
    }
    if (cmd.hasOption("graphiteHost")) {
      controllerConf.setGraphiteHost(cmd.getOptionValue("graphiteHost"));
    }
    if (cmd.hasOption("graphitePort")) {
      controllerConf.setGraphitePort(cmd.getOptionValue("graphitePort"));
    } else {
      controllerConf.setGraphitePort("0");
    }
    if (cmd.hasOption("c3Host")) {
      controllerConf.setC3Host(cmd.getOptionValue("c3Host"));
    } else {
      controllerConf.setC3Host(DEFAULT_C3_HOST);
    }
    if (cmd.hasOption("c3Port")) {
      controllerConf.setC3Port(cmd.getOptionValue("c3Port"));
    } else {
      controllerConf.setC3Port(Integer.toString(DEFAULT_C3_PORT));
    }
    if (cmd.hasOption("enableAutoWhitelist")) {
      controllerConf.setEnableAutoWhitelist(cmd.getOptionValue("enableAutoWhitelist"));
    }
    if (cmd.hasOption("enableAutoTopicExpansion")) {
      controllerConf.setEnableAutoTopicExpansion(cmd.getOptionValue("enableAutoTopicExpansion"));
    }
    if (cmd.hasOption("patternToExcludeTopics")) {
      controllerConf.setPatternToExcludeTopics(cmd.getOptionValue("patternToExcludeTopics"));
    }
    if (cmd.hasOption("enableSrcKafkaValidation")) {
      controllerConf.setEnableSrcKafkaValidation(cmd.getOptionValue("enableSrcKafkaValidation"));
    }
    if (cmd.hasOption("srcKafkaZkPath")) {
      controllerConf.setSrcKafkaZkPath(cmd.getOptionValue("srcKafkaZkPath"));
    }
    if (cmd.hasOption("destKafkaZkPath")) {
      controllerConf.setDestKafkaZkPath(cmd.getOptionValue("destKafkaZkPath"));
    }
    if (cmd.hasOption("consumerCommitZkPath")) {
      controllerConf.setConsumerCommitZkPath(cmd.getOptionValue("consumerCommitZkPath"));
    }
    if (cmd.hasOption("autoRebalanceDelayInSeconds")) {
      controllerConf.setAutoRebalanceDelayInSeconds(cmd.getOptionValue("autoRebalanceDelayInSeconds"));
    } else {
      controllerConf.setAutoRebalanceDelayInSeconds("120");
    }
    if (cmd.hasOption("refreshTimeInSeconds")) {
      controllerConf.setWhitelistRefreshTimeInSeconds(cmd.getOptionValue("refreshTimeInSeconds"));
    } else {
      controllerConf.setWhitelistRefreshTimeInSeconds(Integer.toString(DEFAULT_WHITELIST_REFRESH_TIME_IN_SECONDS));
    }
    if (cmd.hasOption("initWaitTimeInSeconds")) {
      controllerConf.setInitWaitTimeInSeconds(cmd.getOptionValue("initWaitTimeInSeconds"));
    } else {
      controllerConf.setInitWaitTimeInSeconds(Integer.toString(DEFAULT_INIT_WAIT_TIME_IN_SECONDS));
    }
    if (cmd.hasOption("autoRebalancePeriodInSeconds")) {
      controllerConf.setAutoRebalancePeriodInSeconds(cmd.getOptionValue("autoRebalancePeriodInSeconds"));
    } else {
      controllerConf.setAutoRebalancePeriodInSeconds("0");
    }
    if (cmd.hasOption("autoRebalanceMinIntervalInSeconds")) {
      controllerConf.setAutoRebalanceMinIntervalInSeconds(cmd.getOptionValue("autoRebalanceMinIntervalInSeconds"));
    } else {
      controllerConf.setAutoRebalanceMinIntervalInSeconds(
          Integer.toString(DEFAULT_AUTO_REBALANCE_MIN_INTERVAL_IN_SECONDS));
    }
    if (cmd.hasOption("workloadRefreshPeriodInSeconds")) {
      controllerConf.setWorkloadRefreshPeriodInSeconds(cmd.getOptionValue("workloadRefreshPeriodInSeconds"));
    } else {
      controllerConf.setWorkloadRefreshPeriodInSeconds(Integer.toString(DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS));
    }
    if (cmd.hasOption("autoRebalanceWorkloadRatioThreshold")) {
      controllerConf.setAutoRebalanceWorkloadRatioThreshold(cmd.getOptionValue("autoRebalanceWorkloadRatioThreshold"));
    } else {
      controllerConf.setAutoRebalanceWorkloadRatioThreshold(
          Double.toString(DEFAULT_AUTO_REBALANCE_WORKLOAD_RATIO_THRESHOLD));
    }
    if (cmd.hasOption("maxDedicatedLaggingInstancesRatio")) {
      controllerConf.setMaxDedicatedLaggingInstancesRatio(cmd.getOptionValue("maxDedicatedLaggingInstancesRatio"));
    } else {
      controllerConf.setMaxDedicatedLaggingInstancesRatio(
          Double.toString(DEFAULT_MAX_DEDICATED_LAGGING_INSTANCES_RATIO));
    }
    if (cmd.hasOption("numOffsetThread")) {
      controllerConf.setNumOffsetThread(cmd.getOptionValue("numOffsetThread"));
    } else {
      controllerConf.setNumOffsetThread(Integer.toString(DEFAULT_NUM_OFFSET_THREAD));
    }
    if (cmd.hasOption("offsetRefreshIntervalInSec")) {
      controllerConf.setOffsetRefreshIntervalInSec(cmd.getOptionValue("offsetRefreshIntervalInSec"));
    } else {
      controllerConf.setOffsetRefreshIntervalInSec(Integer.toString(DEFAULT_OFFSET_REFRESH_INTERVAL_IN_SEC));
    }
    if (cmd.hasOption("groupId")) {
      controllerConf.setGroupId(cmd.getOptionValue("groupId"));
    }
    if (cmd.hasOption("backUpToGit")) {
      controllerConf.setBackUpToGit(cmd.getOptionValue("backUpToGit"));
      if (controllerConf.getBackUpToGit()) {
        if (cmd.hasOption("remoteBackupRepo")) {
          controllerConf.setRemoteBackupRepo(cmd.getOptionValue("remoteBackupRepo"));
        } else {
          throw new RuntimeException("Missing option: --remoteBackupRepo");
        }

        if (cmd.hasOption("localGitRepoClonePath")) {
          controllerConf.setLocalGitRepoPath(cmd.getOptionValue("localGitRepoClonePath"));
        } else {
          throw new RuntimeException("Missing option: --localGitRepoClonePath");
        }
      } else {
        if (cmd.hasOption("localBackupFilePath")) {
          controllerConf.setLocalBackupFilePath(cmd.getOptionValue("localBackupFilePath"));
        } else {
          throw new RuntimeException("Missing option: --localBackupFilePath");
        }
      }
    } else {
      controllerConf.setBackUpToGit("false");
      controllerConf.setLocalBackupFilePath(defaultLocal);
    }

    return controllerConf;
  }

}
