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

import com.uber.stream.kafka.mirrormaker.manager.rest.ManagerRestApplication;
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

  private final Component _component;
  private final Application _controllerRestApp;

  public ManagerStarter() {
    LOGGER.info("Trying to init ManagerStarter");
    _component = new Component();
    _controllerRestApp = new ManagerRestApplication(null);
  }

  public void start() throws Exception {
    _component.getServers().add(Protocol.HTTP, Integer.parseInt("23842"));
    _component.getClients().add(Protocol.FILE);
    _component.getClients().add(Protocol.JAR);
    _component.getClients().add(Protocol.WAR);

    final Context applicationContext = _component.getContext().createChildContext();

    LOGGER.info("injecting conf and resource manager to the api context");
    _controllerRestApp.setContext(applicationContext);
    _component.getDefaultHost().attach(_controllerRestApp);

    try {
      LOGGER.info("Starting uReplicator Manager");
      LOGGER.info("Starting API component");
      _component.start();
    } catch (final Exception e) {
      LOGGER.error("Caught exception while uReplicator Manager", e);
      throw e;
    }
  }

  public void stop() {
    try {
      LOGGER.info("Stopping API component");
      _component.stop();
    } catch (final Exception e) {
      LOGGER.error("Caught exception", e);
    }
  }

  public static ManagerStarter init() {
    final ManagerStarter starter = new ManagerStarter();
    return starter;
  }

  public static void main(String[] args) throws Exception {

    final ManagerStarter managerStarter = ManagerStarter.init();

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
      LOGGER.error("Cannot start uReplicator Manager: ", e);
    }
  }

}
