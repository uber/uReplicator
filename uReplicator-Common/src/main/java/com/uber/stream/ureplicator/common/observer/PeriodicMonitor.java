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
package com.uber.stream.ureplicator.common.observer;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for periodic monitor
 */
public abstract class PeriodicMonitor {

  public static final String METRIC_PERIODIC_MONITOR_FAILURE_PREFIX = "periodicMonitorFailure_";

  private static final Logger logger = LoggerFactory.getLogger(PeriodicMonitor.class);
  private final ScheduledExecutorService cronExecutor;
  private final int refreshIntervalMs;
  private final String monitorName;
  public final Meter periodicMonitorFailureMeter;

  public PeriodicMonitor(int refreshIntervalMs, String monitorName) {

    this.refreshIntervalMs = refreshIntervalMs;
    this.monitorName = monitorName;
    this.cronExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(monitorName).build());
    this.periodicMonitorFailureMeter  = new Meter();
  }

  public void start() {
    logger.info("Start periodic monitor: {}", monitorName);
    KafkaUReplicatorMetricsReporter.get()
        .registerMetric(METRIC_PERIODIC_MONITOR_FAILURE_PREFIX + monitorName,
            periodicMonitorFailureMeter);
    if (refreshIntervalMs > 0) {
      cronExecutor.scheduleAtFixedRate(() -> {
        try {
          logger.info("Update updateDataSet periodically for {}", monitorName);
          updateDataSet();
        } catch (Exception e) {
          logger.warn("Failed to updateDataSet for {}", monitorName, e);
          periodicMonitorFailureMeter.mark();
        }
      }, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }
  }

  public void shutdown() {
    cronExecutor.shutdown();
  }

  abstract void updateDataSet();
}
