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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Manager configs:
 * Helix configs, Manager Rest layer and reporting.
 *
 * @author hongxu
 */
public class ManagerConf extends PropertiesConfiguration {

  private static final String MANAGER_ZK_STR = "manager.zk.str";
  private static final String MANAGER_PORT = "manager.port";
  private static final String MANAGER_DEPLOYMENT = "manager.deployment";
  private static final String MANAGER_INSTANCE_ID = "manager.instance.id";

  public ManagerConf() {
    super();
    this.setDelimiterParsingDisabled(true);
  }

  public void setManagerZkStr(String zkStr) {
    setProperty(MANAGER_ZK_STR, zkStr);
  }

  public void setManagerPort(String port) {
    setProperty(MANAGER_PORT, port);
  }

  public void setManagerDeployment(String deployment) {
    setProperty(MANAGER_DEPLOYMENT, deployment);
  }

  public void setManagerInstanceId(String instanceId) {
    setProperty(MANAGER_INSTANCE_ID, instanceId);
  }

  public String getManagerZkStr() {
    return (String) getProperty(MANAGER_ZK_STR);
  }

  public String getManagerPort() {
    return (String) getProperty(MANAGER_PORT);
  }

  public String getManagerDeployment() {
    return (String) getProperty(MANAGER_DEPLOYMENT);
  }

  public String getManagerInstanceId() {
    return (String) getProperty(MANAGER_INSTANCE_ID);
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
        .addOption("zookeeper", true, "Zookeeper path")
        .addOption("port", true, "Manager port number")
        .addOption("deployment", true, "Manager deployment")
        .addOption("instanceId", true, "InstanceId");
    return managerOptions;
  }

  public static ManagerConf getManagerConf(CommandLine cmd) {
    ManagerConf managerConf = new ManagerConf();
    if (cmd.hasOption("zookeeper")) {
      managerConf.setManagerZkStr(cmd.getOptionValue("zookeeper"));
    } else {
      throw new RuntimeException("Missing option: --zookeeper");
    }
    if (cmd.hasOption("port")) {
      managerConf.setManagerPort(cmd.getOptionValue("port"));
    } else {
      throw new RuntimeException("Missing option: --port");
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

    return managerConf;
  }

}
