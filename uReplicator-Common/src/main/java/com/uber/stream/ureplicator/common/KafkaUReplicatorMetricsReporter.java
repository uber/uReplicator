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
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Holds a singleton MetricRegistry to be shared across uReplicator.
 *
 * This will start a JmxReporter and an optional GraphiteReporter (based on the config). Note: There
 * is no need to explicitly close this.
 */
public class KafkaUReplicatorMetricsReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUReplicatorMetricsReporter.class);
  private static KafkaUReplicatorMetricsReporter METRICS_REPORTER_INSTANCE = null;

  private static volatile boolean DID_INIT = false;
  private static final String KAFKA_UREPLICATOR_METRICS_REPORTER_PREFIX_FORMAT = "stats.%s.counter.%s.%s";

  // FIXME: change convention for member variable
  private final MetricRegistry _registry;
  private final GraphiteReporter _graphiteReporter;
  private final JmxReporter _jmxReporter;
  private String _reporterMetricPrefix;

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
    _registry = new MetricRegistry();

    // Init jmx reporter
    if (conf.getEnabledGraphiteReport()) {
      _jmxReporter = JmxReporter.forRegistry(this._registry).build();
      _jmxReporter.start();
    } else {
      _jmxReporter = null;
    }

    // Init graphite reporter
    if (conf.getEnabledJmxReport()) {
      Graphite graphite = getGraphite(conf);
      if (graphite == null) {
        _graphiteReporter = null;
      } else {
        _reporterMetricPrefix = String
                .format(KAFKA_UREPLICATOR_METRICS_REPORTER_PREFIX_FORMAT, conf.getRegion(),
                        String.join(".", conf.getAdditionalInfo()),
                        conf.getHostname());
        LOGGER.info("Reporter Metric Prefix is : {}", _reporterMetricPrefix);
        _graphiteReporter =
            GraphiteReporter.forRegistry(_registry).prefixedWith(_reporterMetricPrefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL).build(graphite);
        _graphiteReporter.start(conf.getGraphiteReportFreqSec(), TimeUnit.SECONDS);
      }
    } else {
      _graphiteReporter = null;
    }
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
    if(DID_INIT == false){
      return;
    }
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
    checkState();
    for (MetricName metricName : metrics.keySet()) {
      String kafkaMetricName = String.format("%s.%s", prefix, metricName.name());
      registerMetric(kafkaMetricName, new GraphiteKafkaGauge(metrics.get(metricName)));
    }
  }

  public void removeKafkaMetrics(String prefix, Map<MetricName, ? extends Metric> metrics) {
    checkState();
    for (MetricName metricName : metrics.keySet()) {
      String kafkaMetricName = String.format("%s.%s", prefix, metricName.name());
      removeMetric(kafkaMetricName);
    }
  }

  public static KafkaUReplicatorMetricsReporter get() {
    checkState();
    return METRICS_REPORTER_INSTANCE;
  }

  public MetricRegistry getRegistry() {
    checkState();
    return _registry;
  }

  public <T extends com.codahale.metrics.Metric> void registerMetric(String metricName, T metric) {
    checkState();
    if (_registry == null) {
      return;
    }
    if (!_registry.getNames().contains(metricName)) {
      _registry.register(metricName, metric);
    } else {
      LOGGER.warn("Failed to register an existed metric: {}", metricName);
    }
  }

  public void removeMetric(String metricName) {
    if (_registry == null) {
      return;
    }
    _registry.remove(metricName);
  }

  private static void checkState(){
    Preconditions.checkState(DID_INIT, "Not initialized yet");
  }

  public static boolean isStart(){
    return DID_INIT == true;
  }
}
