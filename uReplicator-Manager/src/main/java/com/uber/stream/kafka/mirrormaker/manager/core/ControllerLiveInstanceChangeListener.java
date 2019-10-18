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
package com.uber.stream.kafka.mirrormaker.manager.core;

import com.codahale.metrics.Counter;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We only considering add or remove box(es), not considering the replacing.
 * For replacing, we just need to bring up a new box and give the old instanceId no auto-balancing needed.
 */
public class ControllerLiveInstanceChangeListener implements LiveInstanceChangeListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerLiveInstanceChangeListener.class);

  private final ControllerHelixManager _controllerHelixManager;
  private final HelixManager _helixManager;

  private final int minIntervalInSeconds = 60;
  private long _lastRebalanceTimeMillis = 0;

  private final ScheduledExecutorService _delayedScheduler = Executors.newSingleThreadScheduledExecutor();

  private final Counter _isLeaderCounter = new Counter();

  public ControllerLiveInstanceChangeListener(ControllerHelixManager controllerHelixManager,
                                              HelixManager helixManager, int _workloadRefreshPeriodInSeconds) {
    _controllerHelixManager = controllerHelixManager;
    _helixManager = helixManager;
    registerMetrics();

    LOGGER.info("Trying to schedule auto rebalancing");
    _delayedScheduler.scheduleWithFixedDelay(
            new Runnable() {
              @Override
              public void run() {
                try {
                  rebalanceCurrentCluster(false);
                } catch (Exception e) {
                  LOGGER.error("Got exception during periodically rebalancing the whole cluster! ", e);
                }
              }
            }, 60, _workloadRefreshPeriodInSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void onLiveInstanceChange(final List<LiveInstance> liveInstances, NotificationContext changeContext) {
    LOGGER.info("ControllerLiveInstanceChangeListener.onLiveInstanceChange() wakes up!");
    _delayedScheduler.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          rebalanceCurrentCluster(true);
        } catch (Exception e) {
          LOGGER.error("Got exception during rebalance the whole cluster! ", e);
        }
      }
    }, 5, TimeUnit.SECONDS);
  }

  public synchronized void rebalanceCurrentCluster(boolean onlyCheckOffline) {
    if (_helixManager.isLeader()) {
      _isLeaderCounter.inc(1 - _isLeaderCounter.getCount());
    } else {
      LOGGER.info("Not leader, do nothing!");
      return;
    }

    if (HelixUtils.liveInstances(_helixManager).isEmpty() ||
            HelixUtils.liveInstances(_controllerHelixManager.getWorkerHelixManager().getHelixManager()).isEmpty()) {
      LOGGER.info("No live instances, do nothing!");
      return;
    }

    LOGGER.info("onlyCheckOffline={}", onlyCheckOffline);
    try {
      _controllerHelixManager.handleLiveInstanceChange(onlyCheckOffline, false);
    } catch (Exception e) {
      LOGGER.error("Failed to handle live instance change!", e);
    }

  }

  private void registerMetrics() {
    try {
      KafkaUReplicatorMetricsReporter.get().registerMetric("leader.counter",
              _isLeaderCounter);
    } catch (Exception e) {
      LOGGER.error("Error registering metrics!", e);
    }
  }

}