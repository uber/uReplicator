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
package com.uber.stream.kafka.mirrormaker.controller.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GitBackUpHandler backs up data in local git repo
 */
public class GitBackUpHandler extends BackUpHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GitBackUpHandler.class);

  private String remotePath = "";
  private String localPath = "";

  public GitBackUpHandler(String remotePath, String localPath) {
    this.remotePath = remotePath;
    this.localPath = localPath;
  }

  public void writeToFile(String fileName, String data) throws Exception {
    Repository backupRepo = null;
    BufferedWriter output = null;
    Git git = null;
    Git result = null;
    try {
      try {
        FileUtils.deleteDirectory(new File(localPath));
      } catch (IOException e) {
        LOGGER.error("Error deleting exisiting backup directory");
        throw e;
      }

      try {
        result = Git.cloneRepository().setURI(remotePath).setDirectory(new File(localPath)).call();
      } catch (Exception e) {
        LOGGER.error("Error cloning backup git repo");
        throw e;
      }

      try {
        backupRepo = new FileRepository(localPath + "/.git");
      } catch (IOException e) {
        throw e;
      }

      git = new Git(backupRepo);
      File myfile = new File(localPath + "/" + fileName);

      try {
        output = new BufferedWriter(new FileWriter(myfile));
        output.write(data);
        output.flush();
      } catch (IOException e) {
        LOGGER.error("Error writing backup to the file with name " + fileName);
        throw e;
      }

      try {
        git.add().addFilepattern(".").call();
      } catch (GitAPIException e) {
        LOGGER.error("Error adding files to git");
        throw e;
      }

      try {
        git.commit().setMessage("Taking backup on " + new Date()).call();

      } catch (GitAPIException e) {
        LOGGER.error("Error commiting files to git");
        throw e;
      }

      try {
        git.push().call();
      } catch (GitAPIException e) {
        LOGGER.error("Error pushing files to git");
        throw e;
      }
    } catch (Exception e) {
      throw e;
    } finally {
      output.close();
      git.close();
      if (result != null)
        result.getRepository().close();
      backupRepo.close();
    }
  }

}
