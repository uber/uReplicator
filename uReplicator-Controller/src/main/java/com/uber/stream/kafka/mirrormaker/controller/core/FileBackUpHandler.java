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
package com.uber.stream.kafka.mirrormaker.controller.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.uber.stream.kafka.mirrormaker.controller.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileBackUpHandler backs up data in local file
 */
public class FileBackUpHandler extends BackUpHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBackUpHandler.class);

  private final String localPath;

  public FileBackUpHandler(String localPath) {
    this.localPath = localPath;
    FileUtils.mkdirQuietly(this.localPath);
  }

  public void writeToFile(String fileName, String data) throws Exception {
    BufferedWriter output = null;
    try {
      File myfile = new File(localPath, fileName);
      try {
        output = new BufferedWriter(new FileWriter(myfile));
        output.write(data);
        output.flush();
        LOGGER.info("Successful backup of file :{}", fileName);
      } catch (IOException e) {
        LOGGER.error("Error writing backup to the file " + fileName, e);
        throw e;
      }
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }

}
