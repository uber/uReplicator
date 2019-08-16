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
package com.uber.streaming.worker.fetcher;

import com.uber.streaming.worker.utils.TypedProperties;
import java.util.Properties;

/**
 * Properties for KafkaConsumerFetcher
 */
public class KafkaConsumerFetcherConfig extends TypedProperties {
  public KafkaConsumerFetcherConfig(Properties p) {
    super.putAll(p);
  }

  public static final String FETCHER_THREAD_BACKOFF_MS = "refresh.backoff.ms";
  public static final int DEFAULT_FETCH_THREAD_BACKOFF_MS = 200;

  public static final String NUMBER_OF_CONSUMER_FETCHERS = "num.consumer.fetchers";
  public static final int DEFAULT_NUMBER_OF_CONSUMER_FETCHERS = 4;

  public static final String OFFSET_MONITOR_INTERVAL = "offset.monitor.ms";
  public static final int DEFAULT_OFFSET_MONITOR_INTERVAL = 60 * 1000;

  public static final String POLL_TIMEOUT_MS = "poll.timeout.ms";
  public static final int DEFAULT_POLL_TIMEOUT_MS = 100;

  // The time, in milliseconds, spent waiting fetcher thread if data is not available in KafkaConsumer.poll.
  public int getFetcherThreadBackoffMs() {
    return getInt(FETCHER_THREAD_BACKOFF_MS, DEFAULT_FETCH_THREAD_BACKOFF_MS);
  }

  // max number of consumer fetcher threads for one consumer group
  public int getNumberOfConsumerFetcher() {
    return getInt(NUMBER_OF_CONSUMER_FETCHERS, DEFAULT_NUMBER_OF_CONSUMER_FETCHERS);
  }

  // The time interval, in milliseconds, to monitor topic partition offset lag
  public int getOffsetMonitorInterval() {
    return getInt(OFFSET_MONITOR_INTERVAL, DEFAULT_OFFSET_MONITOR_INTERVAL);
  }

  // The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
  public int getPollTimeoutMs() {
    return getInt(POLL_TIMEOUT_MS, DEFAULT_POLL_TIMEOUT_MS);
  }


}
