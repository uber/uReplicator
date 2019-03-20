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

import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import org.apache.commons.lang.StringUtils;
import org.restlet.Request;
import org.restlet.data.MediaType;
import org.restlet.data.Method;

public class ControllerRequestURLBuilder {

  private final String _baseUrl;

  private ControllerRequestURLBuilder(String baseUrl) {
    _baseUrl = baseUrl;
  }

  public static ControllerRequestURLBuilder baseUrl(String baseUrl) {
    return new ControllerRequestURLBuilder(baseUrl);
  }

  public Request getTopicExternalViewRequestUrl(String topic) {
    String requestUrl = StringUtils.join(new String[]{
        _baseUrl, "/topics/", topic
    });

    Request request = new Request(Method.GET, requestUrl);
    return request;
  }

  public Request getTopicDeleteRequestUrl(String topic) {
    String requestUrl = StringUtils.join(new String[]{
        _baseUrl, "/topics/", topic
    });

    Request request = new Request(Method.DELETE, requestUrl);
    return request;
  }

  public Request getNoProgressRequestUrl() {
    String requestUrl = StringUtils.join(new String[]{
        _baseUrl, "/noprogress"
    });
    Request request = new Request(Method.GET, requestUrl);
    return request;
  }

  public Request getBlacklistRequestUrl() {
    String requestUrl = StringUtils.join(new String[]{
        _baseUrl, "/blacklist"
    });
    Request request = new Request(Method.GET, requestUrl);
    return request;
  }

  public Request getWorkloadInfoUrl() {
    String requestUrl = StringUtils.join(new String[]{
        _baseUrl, "/admin/workloadinfo"
    });
    Request request = new Request(Method.GET, requestUrl);
    return request;
  }

  public Request postBlacklistRequestUrl(String topic, String partition, String opt) {
    String requestUrl = StringUtils.join(new String[]{
        _baseUrl, String.format("/blacklist?topic=%s&partition=%s&opt=%s", topic, partition, opt)
    });
    Request request = new Request(Method.POST, requestUrl);
    return request;
  }

  public Request getTopicCreationRequestUrl(String topic, int numPartitions) {
    Request request = new Request(Method.POST, _baseUrl + "/topics/");
    TopicPartition topicPartitionInfo = new TopicPartition(topic, numPartitions);
    request.setEntity(topicPartitionInfo.toJSON().toJSONString(), MediaType.APPLICATION_JSON);
    return request;
  }

  public Request getTopicExpansionRequestUrl(String topic, int numPartitions) {
    Request request = new Request(Method.PUT, _baseUrl + "/topics/");
    TopicPartition topicPartitionInfo = new TopicPartition(topic, numPartitions);
    request.setEntity(topicPartitionInfo.toJSON().toJSONString(), MediaType.APPLICATION_JSON);
    return request;
  }
}
