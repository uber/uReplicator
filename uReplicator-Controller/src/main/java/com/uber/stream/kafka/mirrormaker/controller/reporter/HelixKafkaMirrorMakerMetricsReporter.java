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
package com.uber.stream.kafka.mirrormaker.controller.reporter;

import com.codahale.metrics.jmx.*;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Holds a singleton MetricRegistry to be shared across MirrorMaker.
 * This will start a JmxReporter and an optional GraphiteReporter (based on the
 * config). Note: There is no need to explicitly close this.
 */
public class HelixKafkaMirrorMakerMetricsReporter {

  private static HelixKafkaMirrorMakerMetricsReporter METRICS_REPORTER_INSTANCE = null;
  private static final Logger LOGGER = Logger.getLogger(HelixKafkaMirrorMakerMetricsReporter.class);
  private static volatile boolean DID_INIT = false;

  private final MetricRegistry _registry;
  private final GraphiteReporter _graphiteReporter;
  private final JmxReporter _jmxReporter;
  private final String _reporterMetricPrefix;

  // Exposed for tests. Call Metrics.get() instead.
  HelixKafkaMirrorMakerMetricsReporter(ControllerConf config) {
    final String environment = config.getEnvironment();
    final String clientId = StringUtils.isNotEmpty(config.getHostname()) ? config.getHostname() : config.getInstanceId();
    String[] dcNenv = parse(environment);
    if (dcNenv == null) {
      LOGGER.error("Error parsing environment info");
      _registry = null;
      _graphiteReporter = null;
      _jmxReporter = null;
      _reporterMetricPrefix = null;
      return;
    }
    _reporterMetricPrefix = config.isFederatedEnabled() ?
        String.format("stats.%s.counter.%s.%s.%s.%s",
            dcNenv[0], config.getMetricsPrefix(), dcNenv[1], config.getRoute(), clientId) :
        String.format("stats.%s.counter.%s.%s.%s",
            dcNenv[0], config.getMetricsPrefix(), dcNenv[1], clientId);
    LOGGER.info("Reporter Metric Prefix is : " + _reporterMetricPrefix);
    _registry = new MetricRegistry();
    final Boolean enabledGraphiteReporting = true;
    final Boolean enabledJmxReporting = true;
    final long graphiteReportFreqSec = 60L;

    // Init jmx reporter
    if (enabledJmxReporting) {
      _jmxReporter = JmxReporter.forRegistry(this._registry).build();
      _jmxReporter.start();
    } else {
      _jmxReporter = null;
    }

    // Init graphite reporter
    if (enabledGraphiteReporting) {
      Graphite graphite = getGraphite(config);
      if (graphite == null) {
        _graphiteReporter = null;
      } else {
        _graphiteReporter =
            GraphiteReporter.forRegistry(_registry).prefixedWith(_reporterMetricPrefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL).build(graphite);
        _graphiteReporter.start(graphiteReportFreqSec, TimeUnit.SECONDS);
      }
    } else {
      _graphiteReporter = null;
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        stop();
      }
    });
  }

  private String[] parse(String environment) {
    if (environment == null || environment.trim().length() <= 0) {
      return null;
    }
    String[] res = environment.split("\\.");
    if (res == null || res.length != 2) {
      return null;
    }
    return res;
  }

  // Exposed for test
  static Graphite getGraphite(ControllerConf config) {
    if (config.getGraphiteHost() == null || config.getGraphitePort() == 0) {
      LOGGER.warn("No Graphite built!");
      return null;
    }
    InetSocketAddress graphiteAddress =
        new InetSocketAddress(config.getGraphiteHost(), config.getGraphitePort());
    LOGGER.info(String.format("Trying to connect to Graphite with address: %s",
        graphiteAddress.toString()));
    return new Graphite(graphiteAddress);
  }

  /**
   * This function must be called before calling the get() method, because of
   * the dependency on the config object.
   *
   * @param config Specifies config pertaining to Metrics
   */
  public static synchronized void init(ControllerConf config) {
    if (DID_INIT) {
      return;
    }
    METRICS_REPORTER_INSTANCE = new HelixKafkaMirrorMakerMetricsReporter(config);
    DID_INIT = true;
  }

  /**
   * reset metrics report
   */
  public static synchronized void stop() {
    try {
      if (METRICS_REPORTER_INSTANCE._jmxReporter != null) {
        Closeables.close(METRICS_REPORTER_INSTANCE._jmxReporter, true);
      }
      if (METRICS_REPORTER_INSTANCE._graphiteReporter != null) {
        Closeables.close(METRICS_REPORTER_INSTANCE._graphiteReporter, true);
      }
    } catch (Exception e) {
      LOGGER.error("Error while closing Jmx and Graphite reporters.", e);
    }
    DID_INIT = false;

    METRICS_REPORTER_INSTANCE = null;
  }

  public static HelixKafkaMirrorMakerMetricsReporter get() {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    return METRICS_REPORTER_INSTANCE;
  }

  public MetricRegistry getRegistry() {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    return _registry;
  }

  public <T extends com.codahale.metrics.Metric> void registerMetric(String metricName, T metric) {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    if (_registry != null) {
      _registry.register(metricName, metric);
    }
  }

}
