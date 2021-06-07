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
package com.uber.stream.ureplicator.worker;

import com.uber.stream.ureplicator.worker.helix.ControllerWorkerHelixHandler;
import com.uber.stream.ureplicator.worker.helix.ManagerWorkerHelixHandler;
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

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WorkerStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerStarter.class);

  private ManagerWorkerHelixHandler managerWorkerHelixHandler;
  private ControllerWorkerHelixHandler controllerWorkerHelixHandler;
  private WorkerInstance workerInstance;
  private final WorkerConf workerConf;

  protected static final CountDownLatch shutdownLatch = new CountDownLatch(1);

  public WorkerStarter(WorkerConf conf) {
    this(conf, new WorkerInstance(conf));
  }

  public WorkerStarter(WorkerConf conf, WorkerInstance workerInstance) {
    LOGGER.info("Trying to init WorkerStater with config : {}", conf);
    workerConf = conf;
    this.workerInstance = workerInstance;
  }

  public void run() throws Exception {
    Properties helixProps = null;
    try {
      helixProps = WorkerUtils.loadAndValidateHelixProps(workerConf.getHelixConfigFile());
    } catch (Exception e) {
      LOGGER.error(
          "uReplicator Worker failed to start because of load helix config file throws exception",
          e);
      System.exit(1);
    }

    if (workerConf.getFederatedEnabled()) {
      try {
        LOGGER.info("Starting manager-worker listener");
        managerWorkerHelixHandler = new ManagerWorkerHelixHandler(workerConf, helixProps,
            workerInstance);
        managerWorkerHelixHandler.start();
      } catch (Throwable e) {
        LOGGER.error("Caught exception while starting manager-worker listener.", e);
        throw e;
      }
    } else {
      LOGGER.info("Starting worker instance, federated_enabled : {}",
          workerConf.getFederatedEnabled());
      String helixClusterName = helixProps
          .getProperty(Constants.HELIX_CLUSTER_NAME, Constants.DEFAULT_HELIX_CLUSTER_NAME);
      try {
        controllerWorkerHelixHandler = new ControllerWorkerHelixHandler(helixProps,
            helixClusterName, workerInstance);
        controllerWorkerHelixHandler.start();
      } catch (Throwable e) {
        LOGGER.error("Caught exception while starting worker instance.", e);
        throw e;
      }
    }

    runRestApplication();

    // wait until shutdown
    try {
      shutdownLatch.await();
      LOGGER.info("Shutting down worker finished");
    } catch (InterruptedException e) {
      LOGGER.info("Shutting down worker due to interrupted exception");
    }
  }

  // run rest application
  public void runRestApplication() throws Exception {
    if (workerConf.getWorkerPort() == 0) {
      return;
    }
    Component _component = new Component();
    _component.getServers().add(Protocol.HTTP, workerConf.getWorkerPort());
    _component.getClients().add(Protocol.FILE);
    _component.getClients().add(Protocol.JAR);

    Context applicationContext = _component.getContext().createChildContext();
    LOGGER.info("Injecting workerInstance to the api context, port {}", workerConf.getWorkerPort());
    applicationContext.getAttributes().put(WorkerInstance.class.toString(), workerInstance);

    Application restletApplication = new RestletApplication(null);
    restletApplication.setContext(applicationContext);

    _component.getDefaultHost().attach(restletApplication);
    _component.start();
  }

  public void shutdown() {
    if (managerWorkerHelixHandler != null) {
      managerWorkerHelixHandler.shutdown();
    }

    if (controllerWorkerHelixHandler != null) {
      controllerWorkerHelixHandler.shutdown();
    }

    if (workerInstance != null) {
      workerInstance.cleanShutdown();
      workerInstance = null;
    }
    shutdownLatch.countDown();
    LOGGER.info("WorkerStarter stopped.");
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    LOGGER.info("Start WorkerStarter with arguments {}", Arrays.toString(args));
    cmd = parser.parse(WorkerConf.constructWorkerOptions(), args);
    if (cmd.getOptions().length == 0 || cmd.hasOption("help")) {
      HelpFormatter f = new HelpFormatter();
      f.printHelp("OptionsTip", WorkerConf.constructWorkerOptions());
      System.exit(0);
    }

    WorkerConf conf;
    try {
      conf = WorkerConf.getWorkerConf(cmd);
    } catch (Exception e) {
      throw new RuntimeException("Not valid worker configurations!", e);
    }
    final WorkerStarter starter = new WorkerStarter(conf);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          starter.shutdown();
        } catch (Exception e) {
          LOGGER.error("Caught error during shutdown! ", e);
          shutdownLatch.countDown();
        }
      }
    });

    try {
      starter.run();
    } catch (Exception e) {
      LOGGER.error("Cannot start uReplicator worker", e);
    }
  }
}
