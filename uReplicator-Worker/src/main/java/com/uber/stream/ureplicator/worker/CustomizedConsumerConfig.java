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

import java.util.Properties;

/**
 * The customized configuration keys for consumer
 */
public class CustomizedConsumerConfig extends Properties {

  public CustomizedConsumerConfig(Properties p) {
    super.putAll(p);
  }

  public static final String FETCHER_THREAD_BACKOFF_MS = "refresh.backoff.ms";
  public static final int DEFAULT_FETCH_THREAD_BACKOFF_MS = 200;

  public static final String NUMBER_OF_CONSUMER_FETCHERS = "num.consumer.fetchers";
  public static final int DEFAULT_NUMBER_OF_CONSUMER_FETCHERS = 3;

  public static final String CONSUMER_MAX_QUEUE_SIZE = "queued.max.message.chunks";
  public static final int DEFAULT_CONSUMER_MAX_QUEUE_SIZE = 5;

  public static final String OFFSET_MONITOR_INTERVAL = "offset.monitor.ms";
  public static final int DEFAULT_OFFSET_MONITOR_INTERVAL = 60 * 1000;

  public static final String POLL_TIMEOUT_MS = "poll.timeout.ms";
  public static final int DEFAULT_POLL_TIMEOUT_MS = 100;

  public static final String FETCHER_MANAGER_REFRESH_MS = "fetcher.manager.refresh.ms";
  public static final int DEFAULT_FETCHER_MANAGER_REFRESH_MS = 100;

  public static final String CONSUMER_TIMEOUT_MS = "consumer.timeout.ms";
  public static final int DEFAULT_CONSUMER_TIMEOUT_MS = 300;

  public static final String CONSUMER_NUM_OF_MESSAGES_RATE = "consumer.num.of.messages.rate";
  public static final int DEFAULT_CONSUMER_NUM_OF_MESSAGES_RATE= 0;


  public int getFetcherThreadBackoffMs() {
    return getInt(FETCHER_THREAD_BACKOFF_MS, DEFAULT_FETCH_THREAD_BACKOFF_MS);
  }

  public int getNumberOfConsumerFetcher() {
    return getInt(NUMBER_OF_CONSUMER_FETCHERS, DEFAULT_NUMBER_OF_CONSUMER_FETCHERS);
  }

  public int getConsumerMaxQueueSize() {
    return getInt(CONSUMER_MAX_QUEUE_SIZE, DEFAULT_CONSUMER_MAX_QUEUE_SIZE);
  }

  public int getOffsetMonitorInterval() {
    return getInt(OFFSET_MONITOR_INTERVAL, DEFAULT_OFFSET_MONITOR_INTERVAL);
  }

  public int getPollTimeoutMs() {
    return getInt(POLL_TIMEOUT_MS, DEFAULT_POLL_TIMEOUT_MS);
  }

  public int getFetcherManagerRefreshMs() {
    return getInt(FETCHER_MANAGER_REFRESH_MS, DEFAULT_FETCHER_MANAGER_REFRESH_MS);
  }

  public int getConsumerTimeoutMs() {
    return getInt(CONSUMER_TIMEOUT_MS, DEFAULT_CONSUMER_TIMEOUT_MS);
  }

  public int getConsumerNumOfMessageRate() {
    return getInt(CONSUMER_NUM_OF_MESSAGES_RATE, DEFAULT_CONSUMER_NUM_OF_MESSAGES_RATE);
  }

  public int getInt(String key, int defaultValue) {
    try {
      return Integer.parseInt(super.getProperty(key, String.valueOf(defaultValue)));
    } catch (Exception e) {
      return defaultValue;
    }
  }
}
