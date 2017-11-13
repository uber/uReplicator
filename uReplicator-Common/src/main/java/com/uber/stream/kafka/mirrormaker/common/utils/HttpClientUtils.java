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
package com.uber.stream.kafka.mirrormaker.common.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpClientUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtils.class);

  public static ResponseHandler<String> createResponseBodyExtractor() {
    return new ResponseHandler<String>() {
      @Override
      public String handleResponse(final HttpResponse response) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status >= 200 && status < 300) {
          HttpEntity entity = response.getEntity();
          return entity != null ? EntityUtils.toString(entity) : null;
        } else {
          throw new ClientProtocolException(
              "Unexpected response status while getting /topics : " + status);
        }
      }
    };
  }

  public static ResponseHandler<Integer> createResponseCodeExtractor() {
    return new ResponseHandler<Integer>() {
      @Override
      public Integer handleResponse(final HttpResponse response) throws IOException {
        return response.getStatusLine().getStatusCode();
      }
    };
  }

  public static String getData(final HttpClient httpClient,
      final RequestConfig requestConfig,
      final String host,
      final int port,
      final String path) throws IOException, URISyntaxException {
    URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(host)
        .setPort(port)
        .setPath(path)
        .build();

    HttpGet httpGet = new HttpGet(uri);
    httpGet.setConfig(requestConfig);

    return httpClient.execute(httpGet, HttpClientUtils.createResponseBodyExtractor());
  }

  public static int postData(final HttpClient httpClient,
      final RequestConfig requestConfig,
      final String host,
      final int port,
      final String topicName,
      final String src,
      final String dst,
      final int routeId) throws IOException, URISyntaxException {
    URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(host)
        .setPort(port)
        .setPath("/topics/" + topicName)
        .addParameter("src", src)
        .addParameter("dst", dst)
        .addParameter("routeid", String.valueOf(routeId))
        .build();

    HttpPost httpPost = new HttpPost(uri);
    httpPost.setConfig(requestConfig);

    return httpClient.execute(httpPost, HttpClientUtils.createResponseCodeExtractor());
  }

  public static int deleteData(final HttpClient httpClient,
      final RequestConfig requestConfig,
      final String host,
      final int port,
      final String topicName,
      final String src,
      final String dst,
      final int routeId) throws IOException, URISyntaxException {
    URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(host)
        .setPort(port)
        .setPath("/topics/" + topicName)
        .addParameter("src", src)
        .addParameter("dst", dst)
        .addParameter("routeid", String.valueOf(routeId))
        .build();

    HttpDelete httpDelete = new HttpDelete(uri);
    httpDelete.setConfig(requestConfig);

    return httpClient.execute(httpDelete, HttpClientUtils.createResponseCodeExtractor());
  }

}
