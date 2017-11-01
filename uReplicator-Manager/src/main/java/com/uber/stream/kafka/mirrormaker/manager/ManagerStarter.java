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

import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import com.uber.stream.kafka.mirrormaker.manager.core.WorkerHelixManager;
import com.uber.stream.kafka.mirrormaker.manager.rest.ManagerRestApplication;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hongliang on 10/18/17.
 */
public class ManagerStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManagerStarter.class);

  private final ManagerConf _config;
  private final Component _component;
  private final ControllerHelixManager _controllerHelixManager;
  private final WorkerHelixManager _workerHelixManager;

  public ManagerStarter(ManagerConf conf) {
    LOGGER.info("Trying to init ManagerStarter with config: {}", conf);
    _config = conf;
    _component = new Component();
    _controllerHelixManager = new ControllerHelixManager(_config);
    _workerHelixManager = new WorkerHelixManager(_config);
  }

  public void start() throws Exception {
    _component.getServers().add(Protocol.HTTP, _config.getManagerPort());
    _component.getClients().add(Protocol.FILE);
    _component.getClients().add(Protocol.JAR);

    Context applicationContext = _component.getContext().createChildContext();
    LOGGER.info("Injecting conf and helix to the api context");
    applicationContext.getAttributes().put(ControllerHelixManager.class.toString(), _controllerHelixManager);
    Application managerRestApp = new ManagerRestApplication(null);
    managerRestApp.setContext(applicationContext);

    _component.getDefaultHost().attach(managerRestApp);

    try {
      LOGGER.info("Starting helix manager");
      _controllerHelixManager.start();
      _workerHelixManager.start();
      LOGGER.info("Starting API component");
      _component.start();
    } catch (final Exception e) {
      LOGGER.error("Caught exception while starting uReplicator-Manager", e);
      throw e;
    }
  }

  public void stop() {
    try {
      LOGGER.info("Stopping API component");
      _component.stop();
      LOGGER.info("Stopping helix manager");
      _controllerHelixManager.stop();
      _workerHelixManager.stop();
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
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(ManagerConf.constructManagerOptions(), args);
    if (cmd.getOptions().length == 0 || cmd.hasOption("help")) {
      HelpFormatter f = new HelpFormatter();
      f.printHelp("OptionsTip", ManagerConf.constructManagerOptions());
      System.exit(0);
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
