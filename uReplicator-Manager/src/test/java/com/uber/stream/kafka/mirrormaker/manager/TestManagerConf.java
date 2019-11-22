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
package com.uber.stream.kafka.mirrormaker.manager;

import com.uber.stream.kafka.mirrormaker.common.utils.NetUtils;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.testng.annotations.Test;

public class TestManagerConf {

  @Test
  public void testDefaultCmdControllerConf() throws ParseException {
    try {
      String[] args = new String[]{};

      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
      ManagerConf conf = ManagerConf.getManagerConf(cmd);
      Assert.fail("Expected exception to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().equals("Missing option: --zookeeper"));
    }

    try {
      String[] args = new String[]{
          "-zookeeper", "localhost:2181/test"
      };

      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
      ManagerConf conf = ManagerConf.getManagerConf(cmd);
      Assert.fail("Expected exception to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().equals("Missing option: --managerPort"));
    }

    try {
      String[] args = new String[]{
          "-zookeeper", "localhost:2181/test",
          "-managerPort", "9090"
      };

      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
      ManagerConf conf = ManagerConf.getManagerConf(cmd);
      Assert.fail("Expected exception to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().equals("Missing option: --deployment"));
    }

    try {
      String[] args = new String[]{
          "-zookeeper", "localhost:2181/test",
          "-managerPort", "9090",
          "-deployment", "testing"
      };

      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
      ManagerConf conf = ManagerConf.getManagerConf(cmd);
      Assert.fail("Expected exception to be thrown");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().equals("Missing option: --env"));
    }

    try {
      String[] args = new String[]{
          "-zookeeper", "localhost:2181/test",
          "-managerPort", "9090",
          "-deployment", "testing",
          "-env", "dc.testing"
      };

      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
      ManagerConf conf = ManagerConf.getManagerConf(cmd);
      Assert.assertTrue(conf.getSourceClusters().isEmpty());
      Assert.assertTrue(conf.getDestinationClusters().isEmpty());
      Assert.assertEquals(conf.getManagerZkStr(), "localhost:2181/test");
      Assert.assertEquals(conf.getManagerPort().toString(), "9090");
      Assert.assertEquals(conf.getManagerDeployment(), "testing");
      Assert.assertEquals(conf.getManagerInstanceId(), NetUtils.getFirstNoLoopbackIP4Address());
      Assert.assertEquals(conf.getC3Host(), "localhost");
      Assert.assertEquals(conf.getC3Port().toString(), "0");
      Assert.assertEquals(conf.getWorkloadRefreshPeriodInSeconds().toString(), "600");
      Assert.assertEquals(conf.getInitMaxNumPartitionsPerRoute().toString(), "10");
      Assert.assertEquals(conf.getMaxNumPartitionsPerRoute().toString(), "20");
      Assert.assertEquals(conf.getInitMaxNumWorkersPerRoute().toString(), "3");
      Assert.assertEquals(conf.getMaxNumWorkersPerRoute().toString(), "5");
    } catch (Exception e) {
      Assert.fail("No exception is expected");
    }
  }

  @Test
  public void testCmdControllerConf() throws ParseException {
    String[] args = new String[]{
        "-srcClusters", "cluster1,cluster2",
        "-destClusters", "cluster3,cluster4",
        "-zookeeper", "localhost:2181/test",
        "-managerPort", "9090",
        "-deployment", "testing",
        "-env", "dc1.testing",
        "-instanceId", "instance0",
        "-c3Host", "testhost",
        "-c3Port", "8081",
        "-workloadRefreshPeriodInSeconds", "10",
        "-initMaxNumPartitionsPerRoute", "20",
        "-maxNumPartitionsPerRoute", "30",
        "-initMaxNumWorkersPerRoute", "20",
        "-maxNumWorkersPerRoute", "30",
        "-config", "src/test/resources/clusters.properties"
    };
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
    ManagerConf conf = ManagerConf.getManagerConf(cmd);
    Set<String> ret1 = new HashSet<>();
    ret1.add("cluster1");
    ret1.add("cluster2");
    Assert.assertEquals(conf.getSourceClusters(), ret1);
    Set<String> ret2 = new HashSet<>();
    ret2.add("cluster3");
    ret2.add("cluster4");
    Assert.assertEquals(conf.getDestinationClusters(), ret2);
    Assert.assertEquals(conf.getManagerZkStr(), "localhost:2181/test");
    Assert.assertEquals(conf.getManagerPort().toString(), "9090");
    Assert.assertEquals(conf.getManagerDeployment(), "testing");
    Assert.assertEquals(conf.getManagerInstanceId(), "instance0");
    Assert.assertEquals(conf.getC3Host(), "testhost");
    Assert.assertEquals(conf.getC3Port().toString(), "8081");
    Assert.assertEquals(conf.getWorkloadRefreshPeriodInSeconds().toString(), "10");
    Assert.assertEquals(conf.getInitMaxNumPartitionsPerRoute().toString(), "20");
    Assert.assertEquals(conf.getMaxNumPartitionsPerRoute().toString(), "30");
    Assert.assertEquals(conf.getInitMaxNumWorkersPerRoute().toString(), "20");
    Assert.assertEquals(conf.getMaxNumWorkersPerRoute().toString(), "30");
    Assert.assertEquals(conf.getProperty("kafka.cluster.zkStr.cluster1"), "localhost:12026/cluster1");
    Assert.assertEquals(conf.getProperty("kafka.cluster.servers.cluster1"), "localhost:9091");
    Assert.assertEquals(conf.getProperty("kafka.cluster.zkStr.cluster3"), "localhost:12026/cluster3");
    Assert.assertEquals(conf.getProperty("kafka.cluster.servers.cluster3"), "localhost:9092");
  }

}
