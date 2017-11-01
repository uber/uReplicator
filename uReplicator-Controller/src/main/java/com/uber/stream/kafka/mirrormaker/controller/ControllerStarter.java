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
package com.uber.stream.kafka.mirrormaker.controller;

import com.uber.stream.kafka.mirrormaker.controller.core.ManagerControllerHelix;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for everything.
 *
 * @author xiangfu
 */
public class ControllerStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarter.class);
  private final ControllerConf _config;

  private ControllerInstance _controllerInstance = null;
  private final ManagerControllerHelix _managerControllerHelix;

  // shutdown latch for federated mode
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  public ControllerStarter(ControllerConf conf) {
    LOGGER.info("Trying to init ControllerStarter with config: {}", conf);
    _config = conf;
    HelixKafkaMirrorMakerMetricsReporter.init(conf);
    _managerControllerHelix = new ManagerControllerHelix(_config);
  }

  public void start() throws Exception {
    if (_config.isFederatedEnabled()) {
      LOGGER.info("starting Manager-Controller Helix for Federated uRedplicator");
      _managerControllerHelix.start();
      // wait until shutdown
      try {
        shutdownLatch.await();
        LOGGER.info("Shutting down controller");
      } catch (InterruptedException e) {
        LOGGER.info("Shutting down controller due to intterrupted exception");
      }
    } else {
      _controllerInstance = new ControllerInstance(_config);
      try {
        LOGGER.info("starting controller instance");
        _controllerInstance.start();
      } catch (final Exception e) {
        LOGGER.error("Caught exception while starting controller instance", e);
        throw e;
      }
    }
  }

  public void stop() {
    if (_controllerInstance != null) {
      try {
        LOGGER.info("stopping controller instance");
        _controllerInstance.stop();
        _controllerInstance = null;
      } catch (final Exception e) {
        LOGGER.error("Caught exception while stopping controller instance", e);
      }
    }

    if (_config.isFederatedEnabled()) {
      LOGGER.info("stopping Manager-Controller Helix");
      _managerControllerHelix.stop();
      shutdownLatch.countDown();
    }
  }

  public static ControllerConf getDefaultConf() {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerPort("9000");
    conf.setZkStr("localhost:2181");
    conf.setHelixClusterName("testMirrorMaker");
    conf.setDeploymentName("testDeploymentName");
    conf.setBackUpToGit("false");
    conf.setAutoRebalanceDelayInSeconds("120");
    conf.setLocalBackupFilePath("/var/log/kafka-mirror-maker-controller");
    return conf;
  }

  public static ControllerConf getExampleConf() {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerPort("9000");
    conf.setZkStr("localhost:2181");
    conf.setHelixClusterName("testMirrorMaker");
    conf.setDeploymentName("testMirrorMakerDeployment");
    conf.setBackUpToGit("false");
    conf.setAutoRebalanceDelayInSeconds("120");
    conf.setLocalBackupFilePath("/var/log/kafka-mirror-maker-controller");
    conf.setEnableAutoWhitelist("true");
    conf.setEnableAutoTopicExpansion("true");
    conf.setSrcKafkaZkPath("localhost:2181/cluster1");
    conf.setDestKafkaZkPath("localhost:2181/cluster2");
    conf.setInitWaitTimeInSeconds("10");
    conf.setWhitelistRefreshTimeInSeconds("20");
    return conf;
  }

  public static ControllerStarter init(CommandLine cmd) {
    ControllerConf conf = null;
    if (cmd.hasOption("example1")) {
      conf = getDefaultConf();
    } else if (cmd.hasOption("example2")) {
      conf = getExampleConf();
    } else {
      try {
        conf = ControllerConf.getControllerConf(cmd);
      } catch (Exception e) {
        throw new RuntimeException("Not valid controller configurations!", e);
      }
    }
    final ControllerStarter starter = new ControllerStarter(conf);
    return starter;
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    cmd = parser.parse(ControllerConf.constructControllerOptions(), args);
    if (cmd.getOptions().length == 0 || cmd.hasOption("help")) {
      HelpFormatter f = new HelpFormatter();
      f.printHelp("OptionsTip", ControllerConf.constructControllerOptions());
      System.exit(0);
    }
    final ControllerStarter controllerStarter = ControllerStarter.init(cmd);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          controllerStarter.stop();
        } catch (Exception e) {
          LOGGER.error("Caught error during shutdown! ", e);
        }
      }
    });

    try {
      controllerStarter.start();
    } catch (Exception e) {
      LOGGER.error("Cannot start Helix Mirror Maker Controller: ", e);
    }
  }
}
