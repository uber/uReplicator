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
package com.uber.stream.kafka.mirrormaker.starter;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.mirrormaker.controller.ControllerStarter;

import kafka.mirrormaker.MirrorMakerWorker;

/**
 * This is the entry point to start mirror maker controller and worker.
 * The 1st parameter indicates the module to start:
 * - startMirrorMakerController means to start a controller
 * - startMirrorMakerWorker means to start a worker
 * The following parameters are for each module separately.
 */
public class MirrorMakerStarter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MirrorMakerStarter.class);

  public static void main(String[] args) throws Exception {
    if (args.length > 1) {
      if (args[0].equalsIgnoreCase("startMirrorMakerController")) {
        LOGGER.info("Trying to start MirrorMaker Controller with args: {}", Arrays.toString(args));
        ControllerStarter.main(args);
      } else if (args[0].equalsIgnoreCase("startMirrorMakerWorker")) {
        LOGGER.info("Trying to start MirrorMaker Worker with args: {}", Arrays.toString(args));
        MirrorMakerWorker.main(args);
      } else {
        LOGGER.error("Start script should provide the module(startMirrorMakerController/startMirrorMakerWorker)"
            + " to start as the first parameter! Current args: {}", Arrays.toString(args));
      }
    } else {
      LOGGER.error(
          "Start script doesn't provide enough parameters! Current args: {}.", Arrays.toString(args));
    }
  }

}
