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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HeaderWhitelistObserver watches the data change on ZK node to provide the up-to-date header
 * whitelist
 */
public class HeaderWhitelistObserver extends PeriodicMonitor implements IZkDataListener {

  private static final Logger logger = LoggerFactory.getLogger(HeaderWhitelistObserver.class);
  private static final String SEPARATOR = ",";
  private final ZkClient zkClient;
  private final String clusterRootPath;
  private final String headerWhitelistNodePath;

  private Set<String> headerWhitelist;

  /**
   * Constructor
   *
   * @param clusterRootPath root path for zk server
   * @param headerWhitelistNodePath path for header whitelist node
   * @param zkConnectionTimeoutMs zookeeper connection timeout in ms
   * @param zkSessionTimeoutMs zookeeper session timeout in ms
   * @param refreshIntervalMs refresh interval in ms for periodic monitor
   */
  public HeaderWhitelistObserver(String clusterRootPath, String headerWhitelistNodePath,
      int zkConnectionTimeoutMs, int zkSessionTimeoutMs, int refreshIntervalMs) {
    this(new ZkClient(clusterRootPath, zkSessionTimeoutMs, zkConnectionTimeoutMs,
        ZKStringSerializer$.MODULE$), clusterRootPath, headerWhitelistNodePath, refreshIntervalMs);

  }

  @VisibleForTesting
  protected HeaderWhitelistObserver(ZkClient zkClient, String clusterRootPath,
      String headerWhitelistNodePath,
      int refreshIntervalMs) {
    super(refreshIntervalMs, HeaderWhitelistObserver.class.getSimpleName());
    this.zkClient = zkClient;
    this.clusterRootPath = clusterRootPath;
    this.headerWhitelistNodePath = headerWhitelistNodePath;
    this.headerWhitelist = ImmutableSet.of();
  }

  /**
   * Starts HeaderWhitelistObserver
   */
  public void start() {
    logger.info("Starting HeaderWhitelistObserver for zkConnect={} and zkPath={}", clusterRootPath,
        headerWhitelistNodePath);
    super.start();
    zkClient.subscribeDataChanges(headerWhitelistNodePath, this);
    logger.info("HeaderWhitelistObserver started");
  }

  public void shutdown() {
    logger.info("Shutting down HeaderWhitelistObserver for zkConnect={} and zkPath={}",
        clusterRootPath, headerWhitelistNodePath);
    super.shutdown();
    zkClient.unsubscribeAll();
    zkClient.close();
    logger.info("Shutdown HeaderWhitelistObserver finished");
  }

  /**
   * Gets header whitelist from zookeeper and update in-memory cache
   */
  @Override
  public void updateDataSet() {
    String data = zkClient.readData(headerWhitelistNodePath, true);
    if (StringUtils.isEmpty(data)) {
      updateWhitelist(StringUtils.EMPTY);
    } else {
      updateWhitelist(data);
    }
  }

  /**
   * Updates in-memory header whitelist on zNode data change
   *
   * @param path zk path for data change
   * @param dataObject zk node data
   */
  @Override
  public void handleDataChange(String path, Object dataObject) throws Exception {
    logger.info("Update whitelists info by zk update event for zkPath={}", path);
    String data = dataObject == null ? StringUtils.EMPTY : dataObject.toString();
    updateWhitelist(data);
  }

  /**
   * Updates in-memory header whitelist on zNode deletion
   *
   * @param path zk path
   */
  @Override
  public void handleDataDeleted(String path) throws Exception {
    logger.info("Update whitelists info by zk delete event for zkPath={}", path);
    updateWhitelist(StringUtils.EMPTY);
  }

  /**
   * Gets in-memory header whitelist
   */
  public Set<String> getWhitelist() {
    return ImmutableSet.copyOf(headerWhitelist);
  }

  /**
   * Updates in-memory header whitelist
   */
  private synchronized void updateWhitelist(String data) {
    // use trim to remove any leading and trailing whitespace
    String[] newWhitelist = StringUtils.split(data.trim(), SEPARATOR);
    if (newWhitelist.length == 0) {
      headerWhitelist.clear();
      return;
    }
    HashSet<String> newWhitelistSet = new HashSet<>(Arrays.asList(newWhitelist));
    headerWhitelist = ImmutableSet.copyOf(newWhitelistSet);
  }
}
