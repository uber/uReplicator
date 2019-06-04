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
package com.uber.stream.ureplicator.common;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import java.lang.reflect.Field;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Holds a singleton MetricRegistry to be shared across MirrorMaker. This will start a JmxReporter
 * and an optional GraphiteReporter (based on the config). Note: There is no need to explicitly
 * close this.
 */
public class TestKafkaUReplicatorMetricsReporter {

  @Test
  public void testDefaultConf() {

    KafkaUReplicatorMetricsReporter.init(null);
    Assert.assertNull(KafkaUReplicatorMetricsReporter.get().getRegistry());
    try {
      Field field = KafkaUReplicatorMetricsReporter.class.getDeclaredField("DID_INIT");
      field.setAccessible(true);
      field.set(KafkaUReplicatorMetricsReporter.get(), false);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    MetricsReporterConf conf = new MetricsReporterConf("sjc1.sjc1a",
        Collections.singletonList("ureplicator-manager"), null, "localhost",
        4744);
    KafkaUReplicatorMetricsReporter.init(conf);
    Object firstInitInstance = KafkaUReplicatorMetricsReporter.get();
    // Double init will get same instance.
    KafkaUReplicatorMetricsReporter.init(conf);
    Object secondInitInstance = KafkaUReplicatorMetricsReporter.get();
    Assert.assertTrue(firstInitInstance == secondInitInstance);
    Counter testCounter0 = new Counter();
    Meter testMeter0 = new Meter();
    Timer testTimer0 = new Timer();
    KafkaUReplicatorMetricsReporter.get().registerMetric("testCounter0", testCounter0);
    KafkaUReplicatorMetricsReporter.get().registerMetric("testMeter0", testMeter0);
    KafkaUReplicatorMetricsReporter.get().registerMetric("testTimer0", testTimer0);

    testCounter0.inc();
    testMeter0.mark();
    Context context = testTimer0.time();
    context.stop();

    Assert.assertEquals(
        KafkaUReplicatorMetricsReporter.get().getRegistry().getCounters().get("testCounter0")
            .getCount(),
        1);
    Assert.assertEquals(
        KafkaUReplicatorMetricsReporter.get().getRegistry().getMeters().get("testMeter0")
            .getCount(),
        1);
    Assert.assertEquals(
        KafkaUReplicatorMetricsReporter.get().getRegistry().getTimers().get("testTimer0")
            .getCount(),
        1);
  }
}
