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
package com.uber.stream.ureplicator.common.observer;

import org.I0Itec.zkclient.ZkClient;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class HeaderWhitelistObserverTest {

  private ZkClient zkClient = EasyMock.createMock(ZkClient.class);
  private String clusterRootPath = "localhost:2181/test";
  private String headerWhitelistNodePath = "/whitelist";
  private int refreshIntervalMs = 1000;
  private HeaderWhitelistObserver observer = new HeaderWhitelistObserver(zkClient, clusterRootPath,
      headerWhitelistNodePath, refreshIntervalMs);

  @Test
  public void HeaderWhitelistObserverTest() throws Exception {
    zkClient.subscribeDataChanges(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    EasyMock.expect(zkClient.readData(headerWhitelistNodePath, true))
        .andReturn("sourceCluster,destinationCluster");
    EasyMock.replay(zkClient);
    observer.start();
    Thread.sleep(100);
    Assert.assertEquals(2, observer.getWhitelist().size());
    EasyMock.verify(zkClient);

    observer.handleDataChange(headerWhitelistNodePath, "sourceCluster2,destinationCluster");
    Assert.assertEquals(2, observer.getWhitelist().size());

    Assert.assertTrue(observer.getWhitelist().contains("sourceCluster2"));
    Assert.assertTrue(observer.getWhitelist().contains("destinationCluster"));

    observer.handleDataChange(headerWhitelistNodePath, "");
    Assert.assertEquals(0, observer.getWhitelist().size());

    observer.handleDataChange(headerWhitelistNodePath, "sourceCluster2,destinationCluster");
    Assert.assertEquals(2, observer.getWhitelist().size());

    observer.handleDataChange(headerWhitelistNodePath, null);
    Assert.assertEquals(0, observer.getWhitelist().size());

    EasyMock.reset(zkClient);
    zkClient.unsubscribeAll();
    EasyMock.expectLastCall().once();

    zkClient.close();
    EasyMock.expectLastCall().once();
    EasyMock.replay(zkClient);
    observer.shutdown();
    EasyMock.verify(zkClient);
  }

}
