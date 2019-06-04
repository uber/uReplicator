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

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.log4j.Logger;

/**
 * Holds a singleton MetricRegistry to be shared across uReplicator.
 *
 * This will start a JmxReporter and an optional GraphiteReporter (based on the config). Note: There
 * is no need to explicitly close this.
 */
public class KafkaUReplicatorMetricsReporter {

  private static KafkaUReplicatorMetricsReporter METRICS_REPORTER_INSTANCE = null;
  private static final Logger LOGGER = Logger.getLogger(KafkaUReplicatorMetricsReporter.class);
  private static volatile boolean DID_INIT = false;
  // prefix for non federated mode :
  // stats.##dc##.counter.##component##.hostname
  // prefix for federated mode:
  // stats.##dc##.counter.##component##.##federated deployment##.##route id##.hostname
  public static final String KAFKA_UREPLICATOR_METRICS_REPORTER_PREFIX_FORMAT = "stats.%s.counter.%s.%s";

  // FIXME: change convention for member variable
  private final MetricRegistry _registry;
  private final GraphiteReporter _graphiteReporter;
  private final JmxReporter _jmxReporter;
  private final String _reporterMetricPrefix;


  // Exposed for tests. Call Metrics.get() instead.
  KafkaUReplicatorMetricsReporter(MetricsReporterConf conf) {
    if (conf == null) {
      LOGGER.error(
          "Skip create KafkaUReplicatorMetricsReporter because of MetricsReporterConf is null");
      _registry = null;
      _graphiteReporter = null;
      _jmxReporter = null;
      _reporterMetricPrefix = null;
      return;
    }

    _reporterMetricPrefix = String
        .format(KAFKA_UREPLICATOR_METRICS_REPORTER_PREFIX_FORMAT, conf.getRegion(),
            String.join(".", conf.getAdditionalInfo()),
            conf.getHostname());
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
      Graphite graphite = getGraphite(conf);
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

  public static String[] parseEnvironment(String environment) {
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
  static Graphite getGraphite(MetricsReporterConf config) {
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
   * This function must be called before calling the get() method, because of the dependency on the
   * config object.
   *
   * @param config Specifies config pertaining to Metrics
   */
  public static synchronized void init(MetricsReporterConf config) {
    if (DID_INIT) {
      return;
    }
    METRICS_REPORTER_INSTANCE = new KafkaUReplicatorMetricsReporter(config);
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

  public void registerKafkaMetrics(String prefix, Map<MetricName, ? extends Metric> metrics) {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    for (MetricName metricName : metrics.keySet()) {
      String kafkaMetricName = String.format("%s.%s", prefix, metricName.name());
      registerMetric(kafkaMetricName, new GraphiteKafkaGauge(metrics.get(metricName)));
    }
  }

  public static KafkaUReplicatorMetricsReporter get() {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    return METRICS_REPORTER_INSTANCE;
  }

  public MetricRegistry getRegistry() {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    return _registry;
  }

  public <T extends com.codahale.metrics.Metric> void registerMetric(String metricName, T metric) {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    if (_registry == null) {
      return;
    }
    if (!_registry.getNames().contains(metricName)) {
      _registry.register(metricName, metric);
    } else {
      LOGGER.warn("Failed to register an existed metric: {}" + metricName);
    }
  }

  public void removeMetric(String metricName) {
    Preconditions.checkState(DID_INIT, "Not initialized yet");
    if (_registry == null) {
      return;
    }
    _registry.remove(metricName);
  }
}
