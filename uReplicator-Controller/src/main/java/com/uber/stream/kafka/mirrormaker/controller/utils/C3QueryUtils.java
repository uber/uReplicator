/*
 * Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.core.TopicWorkload;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C3QueryUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(C3QueryUtils.class);

  private static final String DEFAULT_QUERY_PATH = "/chaperone3/rawmetrics?";
  private static final long DEFAULT_QUERY_PERIOD_SEC = 3600;
  private static final long DEFAULT_QUERY_MINIMUM_END_TO_CURRENT_SEC = 600;
  private static final int DEFAULT_BATCH_TOPICS = 100;

  public static Map<String, TopicWorkload> retrieveTopicInRate(String c3Host, int c3Port, String kafkaCluster,
      List<String> topics) throws IOException {
    Map<String, TopicWorkload> workloads = new HashMap<>();
    if (c3Port == 0) {
      return workloads;
    }
    long endSec = (System.currentTimeMillis() / 1000 - DEFAULT_QUERY_MINIMUM_END_TO_CURRENT_SEC) / 600 * 600;
    long startSec = endSec - DEFAULT_QUERY_PERIOD_SEC;
    for (int i = 0; i < topics.size(); i += DEFAULT_BATCH_TOPICS) {
      StringBuilder query = new StringBuilder();
      query.append("startSec=");
      query.append(startSec);
      query.append("&endSec=");
      query.append(endSec);
      query.append("&tier=");
      query.append(kafkaCluster);
      query.append("&topicList=");
      List<String> batch = topics.subList(i, Math.min(i + DEFAULT_BATCH_TOPICS, topics.size()));
      query.append(StringUtils.join(batch, ","));
      String jsonStr = makeQuery(c3Host, c3Port, query.toString());
      extractJsonResults(jsonStr, batch, workloads);
    }
    return workloads;
  }

  static void extractJsonResults(String jsonStr, List<String> topics, Map<String, TopicWorkload> workloads) {
    try {
      JSONObject jsonObj = JSON.parseObject(jsonStr);
      if (jsonObj == null) {
        LOGGER.info("Failed to parse C3 result: " + jsonStr);
        return;
      }
      long now = System.currentTimeMillis();
      for (String topic : topics) {
        JSONArray arr = jsonObj.getJSONArray(topic);
        if (arr == null || arr.size() == 0) {
          LOGGER.info("No info in C3 result for topic '" + topic + "'");
          continue;
        }
        JSONObject metrics = arr.getJSONObject(0);
        if (metrics == null) {
          LOGGER.info("Failed to parse C3 result for topic '" + topic + "'");
          continue;
        }
        Long startTimeSec = metrics.getLong("startTimeSec");
        Long endTimeSec = metrics.getLong("endTimeSec");
        Long totalBytes = metrics.getLong("totalBytes");
        Long totalCount = metrics.getLong("totalCount");
        if (startTimeSec == null || endTimeSec == null || totalBytes == null || totalCount == null) {
          LOGGER.info("Failed to parse C3 result for topic '" + topic + "'");
          continue;
        }
        double period = endTimeSec - startTimeSec;
        if (period <= 0) {
          LOGGER.info("Invalid C3 result for topic '" + topic + "': startTimeSec=" + startTimeSec + ","
              + " endTimeSec=" + endTimeSec);
          continue;
        }
        TopicWorkload tw = new TopicWorkload(totalBytes / period, totalCount / period);
        tw.setLastUpdate(now);
        workloads.put(topic, tw);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to extract workload information from JSON: " + jsonStr, e);
    }
  }

  private static String makeQuery(String c3Host, int c3Port, String query) throws IOException {
    String url = "http://" + c3Host + ":" + c3Port + DEFAULT_QUERY_PATH + query;
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(url);
    HttpResponse response = client.execute(request);
    try {
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
      String line;
      StringBuffer sb = new StringBuffer();
      while ((line = rd.readLine()) != null) {
        sb.append(line);
      }
      return sb.toString();
    } finally {
      if (response.getEntity() != null) {
        EntityUtils.consume(response.getEntity());
      }
    }
  }

}
