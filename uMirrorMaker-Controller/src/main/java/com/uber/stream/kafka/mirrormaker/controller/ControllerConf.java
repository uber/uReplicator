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
  private static final String SRC_KAFKA_ZK_PATH = "controller.srckafka.zkStr";
  private static final String DEST_KAFKA_ZK_PATH = "controller.destkafka.zkStr";
  private static final String ENABLE_AUTO_WHITELIST = "controller.enable.auto.whitelist";
  private static final String ENABLE_AUTO_TOPIC_EXPANSION =
      "controller.enable.auto.topic.expansion";

  private static final String ENABLE_SRC_KAFKA_VALIDATION =
      "controller.enable.src.kafka.validation";
  private static final String BACKUP_TO_GIT = "controller.backup.to.git";

  private static final String REMOTE_BACKUP_REPO = "controller.remote.backup.git";
  private static final String LOCAL_GIT_REPO = "controller.local.git.repo";
  private static final String LOCAL_FILE_BACKUP = "controller.local.backup.file.path";

  private static final String PATTERN_TO_EXCLUDE_TOPICS = "controller.pattern.exclude.topics";

  private static final String AUTO_REBALANCE_DELAY_IN_SECONDS =
      "controller.auto.rebalance.delay.in.seconds";

  private static final int DEFAULT_AUTO_REBALANCE_DELAY_IN_SECONDS = 120;

  private static final String REFRESH_TIME_IN_SECONDS = "controller.refresh.time.in.seconds";

  private static final int DEFAULT_REFRESH_TIME_IN_SECONDS = 600;

  private static final String INIT_WAIT_TIME_IN_SECONDS = "controller.init.wait.time.in.seconds";

  private static final int DEFAULT_INIT_WAIT_TIME_IN_SECONDS = 120;

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

  public void setSrcKafkaZkPath(String srcKafkaZkPath) {
    setProperty(SRC_KAFKA_ZK_PATH, srcKafkaZkPath);
  }

  public void setDestKafkaZkPath(String destKafkaZkPath) {
    setProperty(DEST_KAFKA_ZK_PATH, destKafkaZkPath);
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

  public void setRefreshTimeInSeconds(String refreshTimeInSeconds) {
    setProperty(REFRESH_TIME_IN_SECONDS, Integer.parseInt(refreshTimeInSeconds));
  }

  public void setInitWaitTimeInSeconds(String initWaitTimeInSeconds) {
    setProperty(INIT_WAIT_TIME_IN_SECONDS, Integer.parseInt(initWaitTimeInSeconds));
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

  public String getSrcKafkaZkPath() {
    return (String) getProperty(SRC_KAFKA_ZK_PATH);
  }

  public String getDestKafkaZkPath() {
    return (String) getProperty(DEST_KAFKA_ZK_PATH);
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

  public Integer getRefreshTimeInSeconds() {
    if (containsKey(REFRESH_TIME_IN_SECONDS)) {
      return (Integer) getProperty(REFRESH_TIME_IN_SECONDS);
    } else {
      return DEFAULT_REFRESH_TIME_IN_SECONDS;
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
        .addOption("enableAutoWhitelist", true, "Enable Auto Whitelist")
        .addOption("enableAutoTopicExpansion", true, "Enable Auto Topic Expansion during Source Kafka Validation")
        .addOption("patternToExcludeTopics", true, "Exclude specific topics by pattern")
        .addOption("enableSrcKafkaValidation", true, "Enable Source Kafka Validation")
        .addOption("srcKafkaZkPath", true, "Source Kafka Zookeeper Path")
        .addOption("destKafkaZkPath", true, "Destination Kafka Zookeeper Path")
        .addOption("autoRebalanceDelayInSeconds", true, "Auto Rebalance Delay in seconds")
        .addOption("refreshTimeInSeconds", true, "Controller Refresh Time in seconds")
        .addOption("initWaitTimeInSeconds", true, "Controller Init Delay in seconds")
        .addOption("backUpToGit", true,
            "Backup controller metadata to git (true) or local file (false)")
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
    if (cmd.hasOption("autoRebalanceDelayInSeconds")) {
      controllerConf
          .setAutoRebalanceDelayInSeconds(cmd.getOptionValue("autoRebalanceDelayInSeconds"));
    } else {
      controllerConf.setAutoRebalanceDelayInSeconds("120");
    }
    if (cmd.hasOption("refreshTimeInSeconds")) {
      controllerConf.setInitWaitTimeInSeconds(cmd.getOptionValue("refreshTimeInSeconds"));
    } else {
      controllerConf.setInitWaitTimeInSeconds("600");
    }
    if (cmd.hasOption("initWaitTimeInSeconds")) {
      controllerConf.setInitWaitTimeInSeconds(cmd.getOptionValue("initWaitTimeInSeconds"));
    } else {
      controllerConf.setInitWaitTimeInSeconds("120");
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
