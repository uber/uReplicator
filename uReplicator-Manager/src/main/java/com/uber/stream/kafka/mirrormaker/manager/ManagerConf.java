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
  private static final String MANAGER_ZK_STR = "manager.zk.str";
  private static final String MANAGER_PORT = "manager.port";
  private static final String MANAGER_DEPLOYMENT = "manager.deployment";
  private static final String ENV = "manager.environment";
  private static final String MANAGER_INSTANCE_ID = "manager.instance.id";

  private static final String CONTROLLER_PORT = "controller.port";

  private static final String C3_HOST = "manager.c3.host";
  private static final String DEFAULT_C3_HOST = "localhost";

  private static final String C3_PORT = "manager.c3.port";
  private static final int DEFAULT_C3_PORT = 0;

  private static final String SRC_KAFKA_ZK_PATH = "manager.srckafka.zkStr";

  private static final String WORKLOAD_REFRESH_PERIOD_IN_SECONDS = "manager.workload.refresh.period.in.seconds";
  private static final int DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS = 600;

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

  public void setControllerPort(String port) {
    setProperty(CONTROLLER_PORT, Integer.valueOf(port));
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

  public void setWorkloadRefreshPeriodInSeconds(String workloadRefreshPeriodInSeconds) {
    setProperty(WORKLOAD_REFRESH_PERIOD_IN_SECONDS, Integer.parseInt(workloadRefreshPeriodInSeconds));
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

  public Integer getControllerPort() {
    return (Integer) getProperty(CONTROLLER_PORT);
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

  public Integer getWorkloadRefreshPeriodInSeconds() {
    if (containsKey(WORKLOAD_REFRESH_PERIOD_IN_SECONDS)) {
      return (Integer) getProperty(WORKLOAD_REFRESH_PERIOD_IN_SECONDS);
    } else {
      return DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS;
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
        .addOption("zookeeper", true, "Zookeeper path")
        .addOption("managerPort", true, "Manager port number")
        .addOption("deployment", true, "Manager deployment")
        .addOption("instanceId", true, "InstanceId")
        .addOption("controllerPort", true, "Controller port number")
        .addOption("c3Host", true, "Chaperone3 Host")
        .addOption("c3Port", true, "Chaperone3 Port")
        .addOption("srcKafkaZkPath", true, "Source Kafka Zookeeper Path")
        .addOption("workloadRefreshPeriodInSeconds", true, "The period to refresh workload information in seconds");
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
    if (cmd.hasOption("instanceId")) {
      managerConf.setManagerInstanceId(cmd.getOptionValue("instanceId"));
    } else {
      try {
        managerConf.setManagerInstanceId(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e) {
        throw new RuntimeException("Missing option: --instanceId");
      }
    }
    if (cmd.hasOption("controllerPort")) {
      managerConf.setControllerPort(cmd.getOptionValue("controllerPort"));
    } else {
      throw new RuntimeException("Missing option: --controllerPort");
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
    if (cmd.hasOption("srcKafkaZkPath")) {
      managerConf.setSrcKafkaZkPath(cmd.getOptionValue("srcKafkaZkPath"));
    }
    if (cmd.hasOption("workloadRefreshPeriodInSeconds")) {
      managerConf.setWorkloadRefreshPeriodInSeconds(cmd.getOptionValue("workloadRefreshPeriodInSeconds"));
    } else {
      managerConf.setWorkloadRefreshPeriodInSeconds(Integer.toString(DEFAULT_WORKLOAD_REFRESH_PERIOD_IN_SECONDS));
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
