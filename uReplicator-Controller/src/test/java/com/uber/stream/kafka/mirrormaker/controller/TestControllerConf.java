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
package com.uber.stream.kafka.mirrormaker.controller;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestControllerConf {

  @Test
  public void testDefaultControllerConf() throws UnknownHostException {
    ControllerConf defaultConf = ControllerStarter.getDefaultConf();
    Assert.assertEquals(defaultConf.getAutoRebalanceDelayInSeconds().intValue(), 120);
    Assert.assertEquals(defaultConf.getBackUpToGit().booleanValue(), false);
    Assert.assertEquals(defaultConf.getLocalBackupFilePath(),
        "/var/log/kafka-mirror-maker-controller");
    Assert.assertEquals(defaultConf.getControllerPort(), "9000");
    Assert.assertEquals(defaultConf.getHelixClusterName(), "testMirrorMaker");
    Assert.assertEquals(defaultConf.getZkStr(), "localhost:2181");
    Assert.assertEquals(defaultConf.getInstanceId(), InetAddress.getLocalHost().getHostName());
    Assert.assertEquals(defaultConf.getEnableAutoWhitelist(), false);
    Assert.assertEquals(defaultConf.getEnableSrcKafkaValidation(), false);
  }

  @Test
  public void testCmdControllerConf() throws ParseException {
    String[] args = new String[] {
        "-helixClusterName", "testHelixClusterName",
        "-zookeeper", "localhost:2181",
        "-port", "9090",
        "-mode", "auto",
        "-env", "dc1.cluster1",
        "-instanceId", "instance0",
        "-graphiteHost", "graphiteHost0",
        "-graphitePort", "4844",
        "-srcKafkaZkPath", "localhost:2181/kafka1",
        "-destKafkaZkPath", "localhost:2181/kafka2",
        "-enableAutoWhitelist", "true",
        "-autoRebalanceDelayInSeconds", "120",
        "-patternToExcludeTopics", "__*",
        "-backUpToGit", "true",
        "-localBackupFilePath", "/tmp/localBackup",
        "-remoteBackupRepo", "git://kafka-mm-controller-backup.git",
        "-localGitRepoClonePath", "/tmp/kafka-mm-controller-backup",
        "-enableSrcKafkaValidation", "true"
    };
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    cmd = parser.parse(ControllerConf.constructControllerOptions(), args);
    ControllerConf conf = ControllerConf.getControllerConf(cmd);
    Assert.assertEquals(conf.getHelixClusterName(), "testHelixClusterName");
    Assert.assertEquals(conf.getZkStr(), "localhost:2181");
    Assert.assertEquals(conf.getControllerPort(), "9090");
    Assert.assertEquals(conf.getControllerMode(), "auto");
    Assert.assertEquals(conf.getEnvironment(), "dc1.cluster1");
    Assert.assertEquals(conf.getInstanceId(), "instance0");
    Assert.assertEquals(conf.getGraphiteHost(), "graphiteHost0");
    Assert.assertEquals(conf.getSrcKafkaZkPath(), "localhost:2181/kafka1");
    Assert.assertEquals(conf.getDestKafkaZkPath(), "localhost:2181/kafka2");
    Assert.assertEquals(conf.getAutoRebalanceDelayInSeconds().intValue(), 120);
    Assert.assertEquals(conf.getEnableAutoWhitelist(), true);
    Assert.assertEquals(conf.getPatternToExcludeTopics(), "__*");
    Assert.assertEquals(conf.getBackUpToGit().booleanValue(), true);
    Assert.assertEquals(conf.getLocalBackupFilePath(), null);
    Assert.assertEquals(conf.getRemoteBackupRepo(), "git://kafka-mm-controller-backup.git");
    Assert.assertEquals(conf.getLocalGitRepoPath(), "/tmp/kafka-mm-controller-backup");
    Assert.assertEquals(conf.getEnableSrcKafkaValidation(), true);
  }

  @Test
  public void testAnotherCmdControllerConf() throws ParseException {
    String[] args = new String[] {
        "-helixClusterName", "testHelixClusterName",
        "-zookeeper", "localhost:2181",
        "-port", "9090",
        "-mode", "auto",
        "-env", "dc1.cluster1",
        "-instanceId", "instance0",
        "-graphiteHost", "graphiteHost0",
        "-srcKafkaZkPath", "localhost:2181/kafka1",
        "-destKafkaZkPath", "localhost:2181/kafka2",
        "-enableAutoWhitelist", "true",
        "-patternToExcludeTopics", "__*",
        "-backUpToGit", "false",
        "-localBackupFilePath", "/tmp/localBackup",
        "-enableSrcKafkaValidation", "true"
    };
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    cmd = parser.parse(ControllerConf.constructControllerOptions(), args);
    ControllerConf conf = ControllerConf.getControllerConf(cmd);
    Assert.assertEquals(conf.getHelixClusterName(), "testHelixClusterName");
    Assert.assertEquals(conf.getZkStr(), "localhost:2181");
    Assert.assertEquals(conf.getControllerPort(), "9090");
    Assert.assertEquals(conf.getControllerMode(), "auto");
    Assert.assertEquals(conf.getEnvironment(), "dc1.cluster1");
    Assert.assertEquals(conf.getInstanceId(), "instance0");
    Assert.assertEquals(conf.getGraphiteHost(), "graphiteHost0");
    Assert.assertEquals(conf.getGraphitePort().intValue(), 0);
    Assert.assertEquals(conf.getSrcKafkaZkPath(), "localhost:2181/kafka1");
    Assert.assertEquals(conf.getDestKafkaZkPath(), "localhost:2181/kafka2");
    Assert.assertEquals(conf.getAutoRebalanceDelayInSeconds().intValue(), 120);
    Assert.assertEquals(conf.getEnableAutoWhitelist(), true);
    Assert.assertEquals(conf.getPatternToExcludeTopics(), "__*");
    Assert.assertEquals(conf.getBackUpToGit().booleanValue(), false);
    Assert.assertEquals(conf.getLocalBackupFilePath(), "/tmp/localBackup");
    Assert.assertEquals(conf.getRemoteBackupRepo(), null);
    Assert.assertEquals(conf.getLocalGitRepoPath(), null);
    Assert.assertEquals(conf.getEnableSrcKafkaValidation(), true);
  }

  @Test
  public void testNoGitBackupCmdControllerConf() throws ParseException, UnknownHostException {
    String[] args = new String[] {
        "-helixClusterName", "testHelixClusterName",
        "-zookeeper", "localhost:2181",
        "-port", "9090",
        "-mode", "auto",
        "-graphiteHost", "graphiteHost0",
        "-srcKafkaZkPath", "localhost:2181/kafka1",
        "-destKafkaZkPath", "localhost:2181/kafka2",
        "-enableAutoWhitelist", "true",
        "-patternToExcludeTopics", "__*",
        "-enableSrcKafkaValidation", "true"
    };
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    cmd = parser.parse(ControllerConf.constructControllerOptions(), args);
    ControllerConf conf = ControllerConf.getControllerConf(cmd);
    Assert.assertEquals(conf.getHelixClusterName(), "testHelixClusterName");
    Assert.assertEquals(conf.getZkStr(), "localhost:2181");
    Assert.assertEquals(conf.getControllerPort(), "9090");
    Assert.assertEquals(conf.getControllerMode(), "auto");
    Assert.assertEquals(conf.getEnvironment(), "env");
    Assert.assertEquals(conf.getInstanceId(), InetAddress.getLocalHost().getHostName());
    Assert.assertEquals(conf.getGraphiteHost(), "graphiteHost0");
    Assert.assertEquals(conf.getGraphitePort().intValue(), 0);
    Assert.assertEquals(conf.getSrcKafkaZkPath(), "localhost:2181/kafka1");
    Assert.assertEquals(conf.getDestKafkaZkPath(), "localhost:2181/kafka2");
    Assert.assertEquals(conf.getAutoRebalanceDelayInSeconds().intValue(), 120);
    Assert.assertEquals(conf.getEnableAutoWhitelist(), true);
    Assert.assertEquals(conf.getPatternToExcludeTopics(), "__*");
    Assert.assertEquals(conf.getBackUpToGit().booleanValue(), false);
    Assert.assertEquals(conf.getLocalBackupFilePath(), "/var/log/kafka-mirror-maker-controller");
    Assert.assertEquals(conf.getRemoteBackupRepo(), null);
    Assert.assertEquals(conf.getLocalGitRepoPath(), null);
    Assert.assertEquals(conf.getEnableSrcKafkaValidation(), true);
    ControllerConf.constructControllerOptions();
  }

}
