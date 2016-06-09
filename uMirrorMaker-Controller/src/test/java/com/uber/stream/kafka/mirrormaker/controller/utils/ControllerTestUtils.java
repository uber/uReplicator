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
package com.uber.stream.kafka.mirrormaker.controller.utils;

import java.util.ArrayList;
import java.util.List;

public class ControllerTestUtils {

  public static List<FakeInstance> addFakeDataInstancesToAutoJoinHelixCluster(
      String helixClusterName, String zkServer, int numInstances, int base) throws Exception {
    List<FakeInstance> ret = new ArrayList<FakeInstance>();
    for (int i = base; i < numInstances + base; ++i) {
      final String instanceId = "Server_localhost_" + i;
      FakeInstance fakeInstance = new FakeInstance(helixClusterName, instanceId, zkServer);
      fakeInstance.start();
      ret.add(fakeInstance);
    }
    return ret;
  }

}
