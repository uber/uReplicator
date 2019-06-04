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
package com.uber.stream.ureplicator.common;

import java.util.List;

public class MetricsReporterConf {

  private final String region;
  private final List<String> additionalInfo;
  private final String hostname;
  private final String graphiteHost;
  private final Integer graphitePort;

  /**
   * Main constructor
   *
   * @param region uReplicator region
   * @param additionalInfo additional information for the metrics such as : component, federated
   * deployment name, route name
   * @param hostname hostname for instance
   * @param graphiteHost graphite host
   * @param graphitePort graphite port
   */
  public MetricsReporterConf(String region, List<String> additionalInfo,
      String hostname, String graphiteHost, Integer graphitePort) {
    this.region = region;
    this.additionalInfo = additionalInfo;
    this.hostname = hostname;
    this.graphiteHost = graphiteHost;
    this.graphitePort = graphitePort;
  }

  public String getRegion() {
    return region;
  }

  public List<String> getAdditionalInfo() {
    return additionalInfo;
  }

  public String getHostname() {
    return hostname;
  }

  public String getGraphiteHost() {
    return graphiteHost;
  }

  public Integer getGraphitePort() {
    return graphitePort;
  }
}
