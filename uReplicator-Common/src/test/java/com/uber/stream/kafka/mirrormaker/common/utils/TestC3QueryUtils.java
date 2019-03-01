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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestC3QueryUtils {

  @Test
  public void testExtractJsonResults() {
    Map<String, TopicWorkload> workloads = new HashMap<>();

    // full results
    C3QueryUtils.extractJsonResults(
        "{\"topic1\":[{\"endTimeSec\":1485991200,\"startTimeSec\":1485988200,\"totalBytes\":30000,\"totalCount\":3000}],"
            + "\"topic2\":[{\"endTimeSec\":1485991200,\"invalidCount\":0,\"maxLatencyFromCreation\":3750,\"meanLatencyFromCreation\":1576.2152157017917,\"p99LatencyFromCreation\":3529,\"startTimeSec\":1485988200,\"totalBytes\":3000000,\"totalCount\":1500}]}",
        Arrays.asList("topic1", "topic2"), workloads);
    TopicWorkload tw1 = workloads.get("topic1");
    Assert.assertEquals(tw1.getBytesPerSecond(), 10, 0.01);
    Assert.assertEquals(tw1.getMsgsPerSecond(), 1, 0.01);
    TopicWorkload tw2 = workloads.get("topic2");
    Assert.assertEquals(tw2.getBytesPerSecond(), 1000, 0.01);
    Assert.assertEquals(tw2.getMsgsPerSecond(), 0.5, 0.01);
    workloads.clear();

    // no result
    C3QueryUtils.extractJsonResults("", Arrays.asList("topic1", "topic2"), workloads);
    Assert.assertTrue(workloads.isEmpty());

    // no result
    C3QueryUtils.extractJsonResults("{}", Arrays.asList("topic1", "topic2"), workloads);
    Assert.assertTrue(workloads.isEmpty());

    // empty result
    C3QueryUtils.extractJsonResults("{\"topic1\":[]}", Arrays.asList("topic1", "topic2"), workloads);
    Assert.assertTrue(workloads.isEmpty());

    // missing partial result for one of the topics
    C3QueryUtils.extractJsonResults(
        "{\"topic1\":[{,\"startTimeSec\":1485988200,\"totalBytes\":30000,\"totalCount\":3000}],"
            + "\"topic2\":[{\"endTimeSec\":1485991200,\"invalidCount\":0,\"maxLatencyFromCreation\":3750,\"meanLatencyFromCreation\":1576.2152157017917,\"p99LatencyFromCreation\":3529,\"startTimeSec\":1485988200,\"totalBytes\":3000000,\"totalCount\":1500}]}",
        Arrays.asList("topic1", "topic2"), workloads);
    tw1 = workloads.get("topic1");
    Assert.assertNull(tw1);
    tw2 = workloads.get("topic2");
    Assert.assertEquals(tw2.getBytesPerSecond(), 1000, 0.01);
    Assert.assertEquals(tw2.getMsgsPerSecond(), 0.5, 0.01);
    workloads.clear();

    // invalid partial result for one of the topics
    C3QueryUtils.extractJsonResults(
        "{\"topic1\":[{\"endTimeSec\":1485988200,\"startTimeSec\":1485988200,\"totalBytes\":30000,\"totalCount\":3000}],"
            + "\"topic2\":[{\"endTimeSec\":1485991200,\"invalidCount\":0,\"maxLatencyFromCreation\":3750,\"meanLatencyFromCreation\":1576.2152157017917,\"p99LatencyFromCreation\":3529,\"startTimeSec\":1485988200,\"totalBytes\":3000000,\"totalCount\":1500}]}",
        Arrays.asList("topic1", "topic2"), workloads);
    tw1 = workloads.get("topic1");
    Assert.assertNull(tw1);
    tw2 = workloads.get("topic2");
    Assert.assertEquals(tw2.getBytesPerSecond(), 1000, 0.01);
    Assert.assertEquals(tw2.getMsgsPerSecond(), 0.5, 0.01);
    workloads.clear();
  }

}
