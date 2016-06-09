/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.reporter;

import java.lang.reflect.Field;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.ControllerStarter;

public class TestHelixKafkaMirrorMakerMetricsReporter {

  @Test
  public void testDefaultConf() {
    ControllerConf conf = ControllerStarter.getDefaultConf();
    {
      HelixKafkaMirrorMakerMetricsReporter.init(conf);
      Assert.assertNull(HelixKafkaMirrorMakerMetricsReporter.get().getRegistry());
      try {
        Field field = HelixKafkaMirrorMakerMetricsReporter.class.getDeclaredField("DID_INIT");
        field.setAccessible(true);
        field.set(HelixKafkaMirrorMakerMetricsReporter.get(), false);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    conf.setEnvironment("sjc1.sjc1a");
    conf.setGraphiteHost("localhost");
    conf.setGraphitePort("4744");
    HelixKafkaMirrorMakerMetricsReporter.init(conf);
    Object firstInitInstance = HelixKafkaMirrorMakerMetricsReporter.get();
    // Double init will get same instance.
    HelixKafkaMirrorMakerMetricsReporter.init(conf);
    Object secondInitInstance = HelixKafkaMirrorMakerMetricsReporter.get();
    Assert.assertTrue(firstInitInstance == secondInitInstance);
    Counter testCounter0 = new Counter();
    Meter testMeter0 = new Meter();
    Timer testTimer0 = new Timer();
    HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("testCounter0", testCounter0);
    HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("testMeter0", testMeter0);
    HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("testTimer0", testTimer0);

    testCounter0.inc();
    testMeter0.mark();
    Context context = testTimer0.time();
    context.stop();

    Assert.assertEquals(
        HelixKafkaMirrorMakerMetricsReporter.get().getRegistry().getCounters().get("testCounter0")
            .getCount(),
        1);
    Assert.assertEquals(
        HelixKafkaMirrorMakerMetricsReporter.get().getRegistry().getMeters().get("testMeter0")
            .getCount(),
        1);
    Assert.assertEquals(
        HelixKafkaMirrorMakerMetricsReporter.get().getRegistry().getTimers().get("testTimer0")
            .getCount(),
        1);
  }
}
