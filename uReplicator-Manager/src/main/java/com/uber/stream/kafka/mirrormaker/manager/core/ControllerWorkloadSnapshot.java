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

import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.modules.ControllerWorkloadInfo;
import com.uber.stream.kafka.mirrormaker.common.utils.HttpClientUtils;

import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A threadsafe helper class that fetches controller workload information for manger and caches in memory.
 */
public class ControllerWorkloadSnapshot {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerWorkloadSnapshot.class);

  private final CloseableHttpClient httpClient;
  private final RequestConfig requestConfig;

  private final Map<String, HostAndPort> pipelineHostInfoMap;
  private final Map<String, ControllerWorkloadInfo> pipelineWorkloadMap;
  private Set<String> failedPipelines = new HashSet<>();
  private static final Counter workloadRefreshFailedCount = new Counter();
  private static final Counter workloadRefreshExecuted = new Counter();
  protected Timer refreshLatencyMsTimer = new Timer(new UniformReservoir());

  public ControllerWorkloadSnapshot(CloseableHttpClient httpClient, RequestConfig requestConfig) {
    this.httpClient = httpClient;
    this.requestConfig = requestConfig;
    this.pipelineHostInfoMap = new ConcurrentHashMap<>();
    this.pipelineWorkloadMap = new ConcurrentHashMap<>();
    registerMetrics();
  }

  private void registerMetrics() {
    try {
      KafkaUReplicatorMetricsReporter.get().registerMetric("workload.refresh.failed.counter",
          workloadRefreshFailedCount);
      KafkaUReplicatorMetricsReporter.get().registerMetric("workload.refresh.executed.counter",
          workloadRefreshExecuted);
    } catch (Exception e) {
      LOGGER.error("Error registering metrics!", e);
    }
  }

  /**
   * Refreshes controller workload info into mem
   *
   * <p/> This method leverages CompletableFuture for batch get to reduce the latency of the method
   */
  public synchronized void refreshWorkloadInfo() {
    Set<String> failedPipelines = new HashSet<>();
    List<CompletableFuture<Void>> futureList = new ArrayList<>();
    long freshStartTime = System.currentTimeMillis();
    for (Map.Entry<String, HostAndPort> entry : pipelineHostInfoMap.entrySet()) {
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                try {
                  refreshWorkloadInfo(entry.getKey(), entry.getValue());
                } catch (Throwable e) {
                  LOGGER.error(String.format("Get workload error when connecting to %s for route %s. No change on number of workers", entry.getValue().getHost(), entry.getValue(), e));
                  failedPipelines.add(entry.getKey());
                }
              });
      futureList.add(future);
    }
    try {
      CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()])).get();
      this.failedPipelines = failedPipelines;
      this.refreshLatencyMsTimer.update(System.currentTimeMillis() - freshStartTime, TimeUnit.MILLISECONDS);
      this.workloadRefreshFailedCount.inc(failedPipelines.size() - workloadRefreshFailedCount.getCount());
      this.workloadRefreshExecuted.inc();
      LOGGER.info(
          "workloadRefreshExecuted finished");
    } catch (Exception e) {
      LOGGER.error("Caught Exception on refreshWorkloadInfo", e);
    }
  }

  private void refreshWorkloadInfo(String instanceId, HostAndPort hostAndPort) throws IOException, URISyntaxException {
    String result = HttpClientUtils.getData(httpClient, requestConfig,
        hostAndPort.getHost(), hostAndPort.getPort(), "/admin/workloadinfo");
    ControllerWorkloadInfo workloadInfo = JSONObject.parseObject(result, ControllerWorkloadInfo.class);

    pipelineWorkloadMap.put(instanceId, workloadInfo);
  }

  public Set<String> getFailedPipelines() {
    return ImmutableSet.copyOf(failedPipelines);
  }

  public Map<String, ControllerWorkloadInfo> getPipelineWorkloadMap() {
    return ImmutableMap.copyOf(pipelineWorkloadMap);
  }

  public synchronized void updatePipelineHostInfoMap(Map<String, HostAndPort> pipelineHostInfoMap) {
    this.pipelineHostInfoMap.clear();
    this.pipelineHostInfoMap.putAll(pipelineHostInfoMap);
    for (String pipeline : pipelineWorkloadMap.keySet()) {
      if (!pipelineHostInfoMap.containsKey(pipeline)) {
        pipelineWorkloadMap.remove(pipeline);
      }
    }

  }
}
