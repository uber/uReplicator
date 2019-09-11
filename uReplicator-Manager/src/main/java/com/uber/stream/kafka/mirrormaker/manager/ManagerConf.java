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
package com.uber.stream.kafka.mirrormaker.manager;

import com.uber.stream.kafka.mirrormaker.common.configuration.IuReplicatorConf;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Manager configs:
 * Helix configs, Manager Rest layer and reporting.
 *
 * @author hongxu
 */
public class ManagerConf extends PropertiesConfiguration implements IuReplicatorConf {

  private static final String CONFIG_FILE = "config.file";
  private static final String CONFIG_KAFKA_SRC_CLUSTERS_KEY = "kafka.source.clusters";
  private static final String CONFIG_KAFKA_DEST_CLUSTERS_KEY = "kafka.destination.clusters";

  private static final String ENABLE_REBALANCE = "manager.enable.rebalance";
  private static final boolean DEFAULT_ENABLE_REBALANCE = true;

  private static final String MANAGER_ZK_STR = "manager.zk.str";
  private static final String MANAGER_PORT = "manager.port";
  private static final String MANAGER_DEPLOYMENT = "manager.deployment";
  private static final String ENV = "manager.environment";
  private static final String MANAGER_INSTANCE_ID = "manager.instance.id";

  private static final String GRAPHITE_HOST = "manager.graphite.host";
  private static final String GRAPHITE_PORT = "manager.graphite.port";

  private static final String METRICS_PREFIX = "controller.metrics.prefix";
  private static final String DEFAULT_METRICS_PREFIX = "ureplicator2-manager";

  //graphiteReportFreqSec
  private static final String GRAPHITE_REPORT_FREQ_SEC = "manager.graphite.report.freq.sec";
  private static final long DEFAULT_GRAPHITE_REPORT_FREQ_SEC = 60;
  //enabledJmxReporting
  private static final String ENABLE_JMX_REPORT= "manager.enable.jmx.report";
  private static final Boolean DEFAULT_ENABLE_JMX_REPORT = true;
  //enabledGraphiteReporting
  private static final String ENABLE_GRAPHITE_REPORT = "manager.enable.graphite.report";
  private static final Boolean DEFAULT_ENABLE_GRAPHITE_REPORT = true;

  private static final String C3_HOST = "manager.c3.host";
  private static final String DEFAULT_C3_HOST = "localhost";

  private static final String C3_PORT = "manager.c3.port";
  private static final int DEFAULT_C3_PORT = 0;

  private static final String CLUSTER_PREFIX_LENGTH = "manager.cluster.prefix.length";
  private static final int DEFAULT_CLUSTER_PREFIX_LENGTH = 0;

  private static final String WORKLOAD_REFRESH_PERIOD_IN_SECONDS = "manager.workload.refresh.period.in.seconds";
  private static final int DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS = 600;

  private static final String INIT_MAX_NUM_PARTITIONS_PER_ROUTE = "manager.init.max.num.partitions.per.route";
  private static final int DEFAULT_INIT_MAX_NUM_PARTITIONS_PER_ROUTE = 10;

  private static final String MAX_NUM_PARTITIONS_PER_ROUTE = "manager.max.num.partitions.per.route";
  private static final int DEFAULT_MAX_NUM_PARTITIONS_PER_ROUTE = 20;

  private static final String INIT_MAX_NUM_WORKERS_PER_ROUTE = "manager.init.max.num.workers.per.route";
  private static final int DEFAULT_INIT_MAX_NUM_WORKERS_PER_ROUTE = 3;

  private static final String MAX_NUM_WORKERS_PER_ROUTE = "manager.max.num.workers.per.route";
  private static final int DEFAULT_MAX_NUM_WORKERS_PER_ROUTE = 5;

  private static final String BYTES_PER_SECOND_DEFAULT = "manager.bytes.per.second.default";
  private static final double DEFAULT_BYTES_PER_SECOND_DEFAULT = 1000.0;

  private static final String MSGS_PER_SECOND_DEFAULT = "manager.msgs.per.second.default";
  private static final double DEFAULT_MSGS_PER_SECOND_DEFAULT = 1;

  private static final String UPDATE_STATUS_COOL_DOWN_MS = "manager.update.status.cool.down";
  private static final int DEFAULT_UPDATE_STATUS_COOL_DOWN_MS = 60000;

  public ManagerConf() {
    super();
    this.setDelimiterParsingDisabled(true);
  }

  public void setConfigFile(String filePath) {
    setProperty(CONFIG_FILE, filePath);
  }

  public void setSourceClusters(String clusters) {
    setProperty(CONFIG_KAFKA_SRC_CLUSTERS_KEY, clusters);
  }

  public void setDestinationClusters(String clusters) {
    setProperty(CONFIG_KAFKA_DEST_CLUSTERS_KEY, clusters);
  }

  public void setEnableRebalance(String enableRebalance) {
    setProperty(ENABLE_REBALANCE, Boolean.valueOf(enableRebalance));
  }

  public void setManagerZkStr(String zkStr) {
    setProperty(MANAGER_ZK_STR, zkStr);
  }

  public void setManagerPort(String port) {
    setProperty(MANAGER_PORT, Integer.valueOf(port));
  }

  public void setManagerDeployment(String deployment) {
    setProperty(MANAGER_DEPLOYMENT, deployment);
  }

  public void setManagerInstanceId(String instanceId) {
    setProperty(MANAGER_INSTANCE_ID, instanceId);
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

  public void setMetricsPrefix(String metricsPrefix) {
    setProperty(METRICS_PREFIX, metricsPrefix);
  }

  public void setGraphiteReportFreqSec(String graphiteReportFreqSec){
    setProperty(GRAPHITE_REPORT_FREQ_SEC, Long.valueOf(graphiteReportFreqSec));
  }

  public void setEnableJmxReport(String enableJmxReport){
    setProperty(ENABLE_JMX_REPORT, Boolean.valueOf(enableJmxReport));
  }

  public void setEnableGraphiteReport(String enableGraphiteReport){
    setProperty(ENABLE_GRAPHITE_REPORT, Boolean.valueOf(enableGraphiteReport));
  }

  public void setC3Host(String C3Host) {
    setProperty(C3_HOST, C3Host);
  }

  public void setC3Port(String C3Port) {
    setProperty(C3_PORT, Integer.valueOf(C3Port));
  }

  public void setClusterPrefixLength(String clusterPrefixLength) {
    setProperty(CLUSTER_PREFIX_LENGTH, Integer.valueOf(clusterPrefixLength));
  }

  public void setWorkloadRefreshPeriodInSeconds(String workloadRefreshPeriodInSeconds) {
    setProperty(WORKLOAD_REFRESH_PERIOD_IN_SECONDS, Integer.parseInt(workloadRefreshPeriodInSeconds));
  }

  public void setInitMaxNumPartitionsPerRoute(String initMaxNumPartitionsPerRoute) {
    setProperty(INIT_MAX_NUM_PARTITIONS_PER_ROUTE, Integer.parseInt(initMaxNumPartitionsPerRoute));
  }

  public void setMaxNumPartitionsPerRoute(String maxNumPartitionsPerRoute) {
    setProperty(MAX_NUM_PARTITIONS_PER_ROUTE, Integer.parseInt(maxNumPartitionsPerRoute));
  }

  public void setInitMaxNumWorkersPerRoute(String initMaxNumWorkersPerRoute) {
    setProperty(INIT_MAX_NUM_WORKERS_PER_ROUTE, Integer.parseInt(initMaxNumWorkersPerRoute));
  }

  public void setMaxNumWorkersPerRoute(String maxNumWorkersPerRoute) {
    setProperty(MAX_NUM_WORKERS_PER_ROUTE, Integer.parseInt(maxNumWorkersPerRoute));
  }

  public void setBytesPerSecondDefault(String bytesPerSecondDefault) {
    setProperty(BYTES_PER_SECOND_DEFAULT, Double.parseDouble(bytesPerSecondDefault));
  }

  public void setMsgsPerSecondDefault(String msgsPerSecondDefault) {
    setProperty(MSGS_PER_SECOND_DEFAULT, Double.parseDouble(msgsPerSecondDefault));
  }

  public void setUpdateStatusCoolDownMs(String updateStatusCoolDownMs) {
    setProperty(UPDATE_STATUS_COOL_DOWN_MS, Integer.parseInt(updateStatusCoolDownMs));
  }

  public String getConfigFile() {
    if (!containsKey(CONFIG_FILE)) {
      return "";
    }
    return (String) getProperty(CONFIG_FILE);
  }

  public Set<String> getSourceClusters() {
    return parseList(CONFIG_KAFKA_SRC_CLUSTERS_KEY, ",");
  }

  public Set<String> getDestinationClusters() {
    return parseList(CONFIG_KAFKA_DEST_CLUSTERS_KEY, ",");
  }

  public Boolean getEnableRebalance() {
    return (Boolean) getProperty(ENABLE_REBALANCE);
  }

  public String getManagerZkStr() {
    return (String) getProperty(MANAGER_ZK_STR);
  }

  public Integer getManagerPort() {
    return (Integer) getProperty(MANAGER_PORT);
  }

  public String getManagerDeployment() {
    return (String) getProperty(MANAGER_DEPLOYMENT);
  }

  public String getManagerInstanceId() {
    return (String) getProperty(MANAGER_INSTANCE_ID);
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

  public Integer getUpdateStatusCoolDownMs() { return (Integer) getProperty(UPDATE_STATUS_COOL_DOWN_MS); }

  public String getMetricsPrefix() {
    if (containsKey(METRICS_PREFIX)) {
      return (String) getProperty(METRICS_PREFIX);
    } else {
      return DEFAULT_METRICS_PREFIX;
    }
  }

  public Long getGraphiteReportFreqSec() {
    if (containsKey(GRAPHITE_REPORT_FREQ_SEC)) {
      return (Long) getProperty(GRAPHITE_REPORT_FREQ_SEC);
    } else {
      return DEFAULT_GRAPHITE_REPORT_FREQ_SEC;
    }
  }

  public Boolean getEnableJmxReport() {
    if (containsKey(ENABLE_JMX_REPORT)) {
      return (Boolean) getProperty(ENABLE_JMX_REPORT);
    } else {
      return DEFAULT_ENABLE_JMX_REPORT;
    }
  }

  public Boolean getEnableGraphiteReport() {
    if (containsKey(ENABLE_GRAPHITE_REPORT)) {
      return (Boolean) getProperty(ENABLE_GRAPHITE_REPORT);
    } else {
      return DEFAULT_ENABLE_GRAPHITE_REPORT;
    }
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
    return null;
  }

  public Integer getClusterPrefixLength() {
    if (containsKey(CLUSTER_PREFIX_LENGTH)) {
      return (Integer) getProperty(CLUSTER_PREFIX_LENGTH);
    } else {
      return DEFAULT_CLUSTER_PREFIX_LENGTH;
    }
  }

  public Integer getWorkloadRefreshPeriodInSeconds() {
    if (containsKey(WORKLOAD_REFRESH_PERIOD_IN_SECONDS)) {
      return (Integer) getProperty(WORKLOAD_REFRESH_PERIOD_IN_SECONDS);
    } else {
      return DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS;
    }
  }

  public Integer getInitMaxNumPartitionsPerRoute() {
    if (containsKey(INIT_MAX_NUM_PARTITIONS_PER_ROUTE)) {
      return (Integer) getProperty(INIT_MAX_NUM_PARTITIONS_PER_ROUTE);
    } else {
      return DEFAULT_INIT_MAX_NUM_PARTITIONS_PER_ROUTE;
    }
  }

  public Integer getMaxNumPartitionsPerRoute() {
    if (containsKey(MAX_NUM_PARTITIONS_PER_ROUTE)) {
      return (Integer) getProperty(MAX_NUM_PARTITIONS_PER_ROUTE);
    } else {
      return DEFAULT_MAX_NUM_PARTITIONS_PER_ROUTE;
    }
  }

  public Integer getInitMaxNumWorkersPerRoute() {
    if (containsKey(INIT_MAX_NUM_WORKERS_PER_ROUTE)) {
      return (Integer) getProperty(INIT_MAX_NUM_WORKERS_PER_ROUTE);
    } else {
      return DEFAULT_INIT_MAX_NUM_WORKERS_PER_ROUTE;
    }
  }

  public Integer getMaxNumWorkersPerRoute() {
    if (containsKey(MAX_NUM_WORKERS_PER_ROUTE)) {
      return (Integer) getProperty(MAX_NUM_WORKERS_PER_ROUTE);
    } else {
      return DEFAULT_MAX_NUM_WORKERS_PER_ROUTE;
    }
  }

  public Double getBytesPerSecondDefault() {
    if (containsKey(BYTES_PER_SECOND_DEFAULT)) {
      return (Double) getProperty(BYTES_PER_SECOND_DEFAULT);
    } else {
      return DEFAULT_BYTES_PER_SECOND_DEFAULT;
    }
  }

  public Double getMsgsPerSecondDefault() {
    if (containsKey(MSGS_PER_SECOND_DEFAULT)) {
      return (Double) getProperty(MSGS_PER_SECOND_DEFAULT);
    } else {
      return DEFAULT_MSGS_PER_SECOND_DEFAULT;
    }
  }

  private Set<String> parseList(String key, String delim) {
    Set<String> clusters = new HashSet<>();
    if (containsKey(key)) {
      String[] values = ((String) getProperty(key)).split(delim);
      for (String value : values) {
        String cluster = value.trim();
        if (!cluster.isEmpty()) {
          clusters.add(cluster);
        }
      }
    }
    return clusters;
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

  public static Options constructManagerOptions() {
    final Options managerOptions = new Options();
    managerOptions.addOption("help", false, "Help")
        .addOption("config", true, "Config file")
        .addOption("srcClusters", true, "Source cluster names for federated uReplicator")
        .addOption("destClusters", true, "Destination cluster names for federated uReplicator")
        .addOption("enableRebalance", true, "Enable rebalance when there is liveinstance change")
        .addOption("zookeeper", true, "Zookeeper path")
        .addOption("managerPort", true, "Manager port number")
        .addOption("deployment", true, "Manager deployment")
        .addOption("env", true, "Manager env")
        .addOption("instanceId", true, "InstanceId")
        .addOption("graphiteHost", true, "GraphiteHost")
        .addOption("graphitePort", true, "GraphitePort")
        .addOption("metricsPrefix", true, "MetricsPrefix")
        .addOption("graphiteReportFreqSec", true, "Graphite report frequency in seconds")
        .addOption("enableJmxReport", true, "enable jmx report")
        .addOption("enableGraphiteReport", true, "enable graphite report")
        .addOption("c3Host", true, "Chaperone3 Host")
        .addOption("c3Port", true, "Chaperone3 Port")
        .addOption("clusterPrefixLength", true, "Cluster prefix length to extract route name from cluster name")
        .addOption("workloadRefreshPeriodInSeconds", true, "The period to refresh workload information in seconds")
        .addOption("initMaxNumPartitionsPerRoute", true, "The max number of partitions when init a route")
        .addOption("maxNumPartitionsPerRoute", true, "The max number of partitions a route can have")
        .addOption("initMaxNumWorkersPerRoute", true, "The max number of workers when init a route")
        .addOption("maxNumWorkersPerRoute", true, "The max number of workers a route can have")
        .addOption("bytesPerSecondDefault", true, "The default value for bytes per second")
        .addOption("msgsPerSecondDefault", true, "The default value for msgs per second")
        .addOption("updateStatusCoolDownMs", true, "The waiting period for next helix status check");
    return managerOptions;
  }

  public static ManagerConf getManagerConf(CommandLine cmd) {
    ManagerConf managerConf = new ManagerConf();
    if (cmd.hasOption("config")) {
      managerConf.setConfigFile(cmd.getOptionValue("config"));
    } else {
      managerConf.setConfigFile("");
    }
    if (cmd.hasOption("srcClusters")) {
      managerConf.setSourceClusters(cmd.getOptionValue("srcClusters"));
    } else {
      managerConf.setSourceClusters("");
    }
    if (cmd.hasOption("destClusters")) {
      managerConf.setDestinationClusters(cmd.getOptionValue("destClusters"));
    } else {
      managerConf.setDestinationClusters("");
    }
    if (cmd.hasOption("enableRebalance")) {
      managerConf.setEnableRebalance(cmd.getOptionValue("enableRebalance"));
    } else {
      managerConf.setEnableRebalance(Boolean.toString(DEFAULT_ENABLE_REBALANCE));
    }
    if (cmd.hasOption("zookeeper")) {
      managerConf.setManagerZkStr(cmd.getOptionValue("zookeeper"));
    } else {
      throw new RuntimeException("Missing option: --zookeeper");
    }
    if (cmd.hasOption("managerPort")) {
      managerConf.setManagerPort(cmd.getOptionValue("managerPort"));
    } else {
      throw new RuntimeException("Missing option: --managerPort");
    }
    if (cmd.hasOption("deployment")) {
      managerConf.setManagerDeployment(cmd.getOptionValue("deployment"));
    } else {
      throw new RuntimeException("Missing option: --deployment");
    }
    if (cmd.hasOption("env")) {
      managerConf.setEnvironment(cmd.getOptionValue("env"));
    } else {
      throw new RuntimeException("Missing option: --env");
    }
    if (cmd.hasOption("instanceId")) {
      managerConf.setManagerInstanceId(cmd.getOptionValue("instanceId"));
    } else {
      try {
        managerConf.setManagerInstanceId(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        throw new RuntimeException("Missing option: --instanceId");
      }
    }
    if (cmd.hasOption("graphiteHost")) {
      managerConf.setGraphiteHost(cmd.getOptionValue("graphiteHost"));
    }
    if (cmd.hasOption("graphitePort")) {
      managerConf.setGraphitePort(cmd.getOptionValue("graphitePort"));
    } else {
      managerConf.setGraphitePort("0");
    }
    if (cmd.hasOption("metricsPrefix")) {
      managerConf.setMetricsPrefix(cmd.getOptionValue("metricsPrefix"));
    } else {
      managerConf.setMetricsPrefix(DEFAULT_METRICS_PREFIX);
    }
    //
    if (cmd.hasOption("graphiteReportFreqSec")) {
      managerConf.setGraphiteReportFreqSec(cmd.getOptionValue("graphiteReportFreqSec"));
    } else{
      managerConf.setGraphiteReportFreqSec(Long.toString(DEFAULT_GRAPHITE_REPORT_FREQ_SEC));
    }
    if (cmd.hasOption("enableJmxReport")) {
      managerConf.setEnableJmxReport(cmd.getOptionValue("enableJmxReport"));
    } else {
      managerConf.setEnableJmxReport(String.valueOf(DEFAULT_ENABLE_JMX_REPORT));
    }
    if (cmd.hasOption("enableGraphiteReport")) {
      managerConf.setEnableGraphiteReport(cmd.getOptionValue("enableGraphiteReport"));
    } else {
      managerConf.setEnableGraphiteReport(String.valueOf(DEFAULT_ENABLE_GRAPHITE_REPORT));
    }
    if (cmd.hasOption("c3Host")) {
      managerConf.setC3Host(cmd.getOptionValue("c3Host"));
    } else {
      managerConf.setC3Host(DEFAULT_C3_HOST);
    }
    if (cmd.hasOption("c3Port")) {
      managerConf.setC3Port(cmd.getOptionValue("c3Port"));
    } else {
      managerConf.setC3Port(Integer.toString(DEFAULT_C3_PORT));
    }
    if (cmd.hasOption("clusterPrefixLength")) {
      managerConf.setClusterPrefixLength(cmd.getOptionValue("clusterPrefixLength"));
    } else {
      managerConf.setClusterPrefixLength(Integer.toString(DEFAULT_CLUSTER_PREFIX_LENGTH));
    }
    if (cmd.hasOption("workloadRefreshPeriodInSeconds")) {
      managerConf.setWorkloadRefreshPeriodInSeconds(cmd.getOptionValue("workloadRefreshPeriodInSeconds"));
    } else {
      managerConf.setWorkloadRefreshPeriodInSeconds(Integer.toString(DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS));
    }
    if (cmd.hasOption("initMaxNumPartitionsPerRoute")) {
      managerConf.setInitMaxNumPartitionsPerRoute(cmd.getOptionValue("initMaxNumPartitionsPerRoute"));
    } else {
      managerConf.setInitMaxNumPartitionsPerRoute(Integer.toString(DEFAULT_INIT_MAX_NUM_PARTITIONS_PER_ROUTE));
    }
    if (cmd.hasOption("maxNumPartitionsPerRoute")) {
      managerConf.setMaxNumPartitionsPerRoute(cmd.getOptionValue("maxNumPartitionsPerRoute"));
    } else {
      managerConf.setMaxNumPartitionsPerRoute(Integer.toString(DEFAULT_MAX_NUM_PARTITIONS_PER_ROUTE));
    }
    if (cmd.hasOption("initMaxNumWorkersPerRoute")) {
      managerConf.setInitMaxNumWorkersPerRoute(cmd.getOptionValue("initMaxNumWorkersPerRoute"));
    } else {
      managerConf.setInitMaxNumWorkersPerRoute(Integer.toString(DEFAULT_INIT_MAX_NUM_WORKERS_PER_ROUTE));
    }
    if (cmd.hasOption("maxNumWorkersPerRoute")) {
      managerConf.setMaxNumWorkersPerRoute(cmd.getOptionValue("maxNumWorkersPerRoute"));
    } else {
      managerConf.setMaxNumWorkersPerRoute(Integer.toString(DEFAULT_MAX_NUM_WORKERS_PER_ROUTE));
    }
    if (cmd.hasOption("bytesPerSecondDefault")) {
      managerConf.setBytesPerSecondDefault(cmd.getOptionValue("bytesPerSecondDefault"));
    } else {
      managerConf.setBytesPerSecondDefault(Double.toString(DEFAULT_BYTES_PER_SECOND_DEFAULT));
    }
    if (cmd.hasOption("msgsPerSecondDefault")) {
      managerConf.setMsgsPerSecondDefault(cmd.getOptionValue("msgsPerSecondDefault"));
    } else {
      managerConf.setMsgsPerSecondDefault(Double.toString(DEFAULT_MSGS_PER_SECOND_DEFAULT));
    }
    if (cmd.hasOption("updateStatusCoolDownMs")) {
      managerConf.setUpdateStatusCoolDownMs(cmd.getOptionValue("updateStatusCoolDownMs"));
    } else {
      managerConf.setUpdateStatusCoolDownMs(Integer.toString(DEFAULT_UPDATE_STATUS_COOL_DOWN_MS));
    }

    if (cmd.hasOption("config")) {
      String fileName = cmd.getOptionValue("config");
      managerConf.setConfigFile(fileName);
      // load config from file
      PropertiesConfiguration configFromFile = new PropertiesConfiguration();
      configFromFile.setDelimiterParsingDisabled(true);
      try {
        configFromFile.load(fileName);
      } catch (ConfigurationException e) {
        throw new RuntimeException("Failed to load config from file " + fileName + ": " + e.getMessage());
      }
      // merge the config with command line. Option from command line has higher priority to override config from file
      @SuppressWarnings("unchecked")
      Iterator<String> keyIter = configFromFile.getKeys();
      while (keyIter.hasNext()) {
        String key = keyIter.next();
        if (!managerConf.containsKey(key)) {
          managerConf.addPropertyDirect(key, configFromFile.getProperty(key));
        }
      }
    } else {
      managerConf.setConfigFile("");
    }

    return managerConf;
  }

}
