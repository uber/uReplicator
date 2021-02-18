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

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerUtils.class);

  public static String getManagerWorkerHelixClusterName(String federatedGroupName) {
    return Constants.MANAGER_WORKER_HELIX_PREFIX + federatedGroupName;
  }

  public static String getControllerWorkerHelixClusterName(String routeName) {
    return Constants.CONTROLLER_WORKER_HELIX_PREFIX + routeName;
  }

  public static Properties loadProperties(String configFile) {
    if (StringUtils.isEmpty(configFile)) {
      return null;
    }

    File file = new File(configFile);
    if (!file.exists()) {
      throw new IllegalArgumentException(String.format("File %s not found", configFile));
    }
    try {
      return Utils.loadProps(configFile);
    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Load config file %s failed", configFile), e);
    }
  }

  public static Properties loadAndValidateHelixProps(String helixConfigFile) {
    Properties properties = loadProperties(helixConfigFile);
    if (!properties.containsKey(Constants.HELIX_ZK_SERVER)) {
      String msg = String
          .format("properties %s required on helix config file: %s", Constants.HELIX_ZK_SERVER,
              helixConfigFile);
      LOGGER.info(msg);
      throw new IllegalArgumentException(msg);
    }

    return properties;
  }

  public static String[] parseSrcDstCluster(String resourceName) {
    if (!resourceName.startsWith(Constants.ROUTE_SEPARATOR)) {
      LOGGER.error("Invalid route : {}", resourceName);
      return null;
    }
    String[] pairs = resourceName.split(Constants.ROUTE_SEPARATOR);
    if (pairs.length != 3) {
      LOGGER.error("Invalid route : {}", resourceName);
      return null;
    }
    return pairs;
  }
}

