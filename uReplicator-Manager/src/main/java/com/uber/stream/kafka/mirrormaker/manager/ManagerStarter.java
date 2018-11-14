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
package com.uber.stream.kafka.mirrormaker.manager;

import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import com.uber.stream.kafka.mirrormaker.manager.reporter.HelixKafkaMirrorMakerMetricsReporter;
import com.uber.stream.kafka.mirrormaker.manager.rest.ManagerRestApplication;
import com.uber.stream.kafka.mirrormaker.manager.validation.SourceKafkaClusterValidationManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static java.lang.System.exit;

/**
 * Created by hongliang on 10/18/17.
 */
public class ManagerStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManagerStarter.class);

  private final ManagerConf _config;
  private final Component _component;
  private final ControllerHelixManager _controllerHelixManager;
  private final SourceKafkaClusterValidationManager _srcKafkaValidationManager;

  public ManagerStarter(ManagerConf conf) {
    LOGGER.info("Trying to init ManagerStarter with config: {}", conf);
    _config = conf;
    HelixKafkaMirrorMakerMetricsReporter.init(conf);
    _component = new Component();
    _srcKafkaValidationManager = new SourceKafkaClusterValidationManager(_config);
    _controllerHelixManager = new ControllerHelixManager(_srcKafkaValidationManager, _config);
  }

  public void start() throws Exception {
    _component.getServers().add(Protocol.HTTP, _config.getManagerPort());
    _component.getClients().add(Protocol.FILE);
    _component.getClients().add(Protocol.JAR);

    Context applicationContext = _component.getContext().createChildContext();
    LOGGER.info("Injecting conf and helix to the api context");
    applicationContext.getAttributes().put(ManagerConf.class.toString(), _config);
    applicationContext.getAttributes().put(ControllerHelixManager.class.toString(), _controllerHelixManager);
    applicationContext.getAttributes()
        .put(SourceKafkaClusterValidationManager.class.toString(), _srcKafkaValidationManager);
    Application managerRestApp = new ManagerRestApplication(null);
    managerRestApp.setContext(applicationContext);

    _component.getDefaultHost().attach(managerRestApp);

    try {
      LOGGER.info("Starting helix manager");
      _controllerHelixManager.start();
      LOGGER.info("Starting source kafka cluster validation manager");
      _srcKafkaValidationManager.start();
      LOGGER.info("Starting API component");
      _component.start();
    } catch (final Exception e) {
      LOGGER.error("Caught exception while starting uReplicator-Manager", e);
      throw e;
    }
  }

  public void stop() {
    try {
      LOGGER.info("Stopping source kafka cluster validation manager");
      _srcKafkaValidationManager.stop();
      LOGGER.info("Stopping API component");
      _component.stop();
      LOGGER.info("Stopping helix manager");
      _controllerHelixManager.stop();
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
    }
  }

  public static ManagerStarter init(CommandLine cmd) {
    ManagerConf conf;
    try {
      conf = ManagerConf.getManagerConf(cmd);
    } catch (Exception e) {
      throw new RuntimeException("Not valid controller configurations!", e);
    }

    return new ManagerStarter(conf);
  }

  public static void main(String[] args) throws Exception {

    HelixManager mgr = HelixSetupUtils.setup("manager-controller-testing-dca1",
        "localhost:2181/ureplicator/testing-dca1", "dxm-test");
    InstanceConfig config = mgr.getConfigAccessor()
        .getInstanceConfig("manager-controller-testing-dca1", "compute1040-dca1");
    String hostname = config.getHostName();
    LOGGER.warn("hostname is ", hostname);
//    List<String> liveInstances = HelixUtils.liveInstances(mgr);
//    HelixDataAccessor da = mgr.getHelixDataAccessor();
//    ZkHelixPropertyStore store = mgr.getHelixPropertyStore();
//    PropertyKey key = da.keyBuilder().instanceConfig("compute1040-dca1");
//    String path = key.getPath();
//    HelixProperty p = da.getProperty(key);
//    String res = p.getRecord().getSimpleField("HELIX_HOST");

    exit(0);
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
    if (cmd.getOptions().length == 0 || cmd.hasOption("help")) {
      HelpFormatter f = new HelpFormatter();
      f.printHelp("OptionsTip", ManagerConf.constructManagerOptions());
      exit(0);
    }
    final ManagerStarter managerStarter = ManagerStarter.init(cmd);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          managerStarter.stop();
        } catch (Exception e) {
          LOGGER.error("Caught error during shutdown! ", e);
        }
      }
    });

    try {
      managerStarter.start();
    } catch (Exception e) {
      LOGGER.error("Cannot start uReplicator-Manager: ", e);
    }
  }

}
