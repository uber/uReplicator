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

import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.KafkaStarterUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import kafka.server.KafkaServerStartable;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SecureWorkerInstanceTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SecureWorkerInstanceTest.class);

    // zk cluster
    private int zkClusterPort = ZkStarter.DEFAULT_ZK_TEST_PORT;
    private String zkCluster1Str = "localhost:" + zkClusterPort;

    // Source Cluster (NonSecure)
    private KafkaServerStartable srcKafka;
    private final int srcClusterPort = 19092;
    private final String srcClusterZK = zkCluster1Str + "/" + TestUtils.SRC_CLUSTER;
    private final String srcClusterBootstrapServer = String
            .format("localhost:%d", srcClusterPort);

    // Source Cluster (Secure)
    private KafkaServerStartable srcKafkaSec;
    private final int srcClusterSecPort = 19082;
    private final String srcClusterSecZK = zkCluster1Str + "/" + TestUtils.SRC_CLUSTER_SEC;
    private final String srcClusterSecBootstrapServer = String
            .format("localhost:%d", srcClusterSecPort);


    // Destination Cluster (NonSecure)
    private KafkaServerStartable dstKafka;
    private final int dstClusterPort = 19093;
    private final String dstClusterZK = zkCluster1Str + "/" + TestUtils.DST_CLUSTER;
    private final String dstClusterBootstrapServer = String
            .format("localhost:%d", dstClusterPort);

    // Destination Cluster (Secure)
    private KafkaServerStartable dstKafkaSec;
    private final int dstClusterSecPort = 19083;
    private final String dstClusterSecZK = zkCluster1Str + "/" + TestUtils.DST_CLUSTER_SEC;
    private final String dstClusterSecBootstrapServer = String
            .format("localhost:%d", dstClusterSecPort);

    private int numberOfPartitions = 2;
    ZKHelixAdmin helixAdmin;
    WorkerConf workerConf;
    Properties helixProps;

    @BeforeClass
    public void setup() throws Exception {
        Properties defaultConfig = KafkaStarterUtils.getDefaultKafkaConfiguration();

        Properties secureConfig = (Properties) KafkaStarterUtils.getDefaultKafkaConfiguration().clone();
        secureConfig.putAll(WorkerUtils.loadProperties("src/test/resources/server-secure.properties"));

        Process p = new ProcessBuilder("sh", "src/test/resources/init_keystore.sh").start();
        p.waitFor();
        Assert.assertEquals(0, p.exitValue());

        ZkStarter.startLocalZkServer();

        LOGGER.info("Starting Source Kafka Cluster1...");
        srcKafka = KafkaStarterUtils.startServer(srcClusterPort, 0, srcClusterZK, defaultConfig);
        srcKafka.startup();

        LOGGER.info("Starting Source Kafka Cluster1 - Secure...");
        srcKafkaSec = KafkaStarterUtils.startServer(srcClusterSecPort, 0, srcClusterSecZK, secureConfig);
        srcKafkaSec.startup();

        LOGGER.info("Starting Destination Kafka Cluster1...");
        dstKafka = KafkaStarterUtils.startServer(dstClusterPort, 0, dstClusterZK, defaultConfig);
        dstKafka.startup();

        LOGGER.info("Starting Destination Kafka Cluster2 - Secure...");
        dstKafkaSec = KafkaStarterUtils.startServer(dstClusterSecPort, 0, dstClusterSecZK, secureConfig);
        dstKafkaSec.startup();

        workerConf = TestUtils.initWorkerConf();
        workerConf.setSecureConfigFile("src/test/resources/secure.properties");
        workerConf.setSecureFeatureEnabled(true);

        helixProps = WorkerUtils.loadAndValidateHelixProps(workerConf.getHelixConfigFile());

        // prepare helix cluster
        String route0 = String.format("%s-%s-0", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
        String route1 = String.format("%s-%s-0", TestUtils.SRC_CLUSTER_SEC, TestUtils.DST_CLUSTER);
        String route2 = String.format("%s-%s-0", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER_SEC);
        String route3 = String.format("%s-%s-0", TestUtils.SRC_CLUSTER_SEC, TestUtils.DST_CLUSTER_SEC);
        helixAdmin = TestUtils.initHelixClustersForWorkerTest(helixProps, route0, route1, route2, route3);
    }

    @Test
    public void testNonSecureSrcAndNonSecureDst() throws Exception {
        testSecureEndToEndCore(false, false);
    }

    @Test
    public void testNonSecureSrcAndSecureDst() throws Exception {
        testSecureEndToEndCore(false, true);
    }

    @Test
    public void testSecureSrcAndNonSecureDst() throws Exception {
        testSecureEndToEndCore(true, false);
    }

    @Test
    public void testSecureSrcAndSecureDst() throws Exception {
        testSecureEndToEndCore(true, true);
    }

    private void testSecureEndToEndCore(boolean isSrcSecure, boolean isDstSecure) throws Exception {
        class WorkerStarterRunnable implements Runnable {

            private final WorkerStarter starter;

            public WorkerStarterRunnable(WorkerConf workerConf) {
                starter = new WorkerStarter(workerConf);
            }

            @Override
            public void run() {
                try {
                    LOGGER.info("Starting WorkerStarter");
                    starter.run();
                } catch (Exception e) {
                    LOGGER.error("WorkerStarter failed", e);
                }
            }

            public void shutdown() {
                starter.shutdown();
            }
        }

        String srcClusterName, srcBootstrapServer, srcZK, dstClusterName, dstBootstrapServer, dstZK, clusterSet;

        int val = 0;
        // 0: (srcNotSec, dstNotSec), 1: (srcSec, dstNotSec), 2: (srcNotSec, dstSec), 3: (srcSec, dstSec)
        if (isSrcSecure) val |= 0x1;
        if (isDstSecure) val |= 0x2;

        String topicName = "testSecureWorkerEndToEnd_" + val;

        switch(val) {
            case 0:
                srcClusterName = TestUtils.SRC_CLUSTER;
                srcBootstrapServer = srcClusterBootstrapServer;
                srcZK = srcClusterZK;

                dstClusterName = TestUtils.DST_CLUSTER;
                dstBootstrapServer = dstClusterBootstrapServer;
                dstZK = dstClusterZK;
                clusterSet = "";
                break;
            case 1:
                srcClusterName = TestUtils.SRC_CLUSTER_SEC;
                srcBootstrapServer = srcClusterSecBootstrapServer;
                srcZK = srcClusterSecZK;

                dstClusterName = TestUtils.DST_CLUSTER;
                dstBootstrapServer = dstClusterBootstrapServer;
                dstZK = dstClusterZK;
                clusterSet = srcClusterName;
                break;
            case 2:
                srcClusterName = TestUtils.SRC_CLUSTER;
                srcBootstrapServer = srcClusterBootstrapServer;
                srcZK = srcClusterZK;

                dstClusterName = TestUtils.DST_CLUSTER_SEC;
                dstBootstrapServer = dstClusterSecBootstrapServer;
                dstZK = dstClusterSecZK;
                clusterSet = dstClusterName;
                break;
            case 3:
                srcClusterName = TestUtils.SRC_CLUSTER_SEC;
                srcBootstrapServer = srcClusterSecBootstrapServer;
                srcZK = srcClusterSecZK;

                dstClusterName = TestUtils.DST_CLUSTER_SEC;
                dstBootstrapServer = dstClusterSecBootstrapServer;
                dstZK = dstClusterSecZK;
                clusterSet = String.format("%s,%s", srcClusterName, dstClusterName);
                break;
            default:
                throw new RuntimeException("Case: " + val + " Not Supported!");
        }

        KafkaStarterUtils.createTopic(topicName, numberOfPartitions, srcZK, "1");
        KafkaStarterUtils.createTopic(topicName, numberOfPartitions, dstZK, "1");

        workerConf.setSecureClustersSet(clusterSet);

        String instanceId = helixProps.getProperty(Constants.HELIX_INSTANCE_ID, null);
        Assert.assertNotNull(instanceId, String
                .format("failed to find property %s in configuration file %s", Constants.HELIX_INSTANCE_ID,
                        workerConf.getHelixConfigFile()));

        // prepare helix cluster
        String route = String.format("%s-%s-0", srcClusterName, dstClusterName);
        String routeForHelix = String.format("@%s@%s", srcClusterName, dstClusterName);

        String controllerHelixClusterName = WorkerUtils.getControllerWorkerHelixClusterName(route);
        // HelixSetupUtils.setup(controllerHelixClusterName, helixProps.getProperty("zkServer"), "0");

        String deployment = helixProps.getProperty("federated.deployment.name");
        String managerHelixClusterName = WorkerUtils.getManagerWorkerHelixClusterName(deployment);

        WorkerStarterRunnable runnable = new WorkerStarterRunnable(workerConf);
        Thread workerThread = new Thread(runnable);
        workerThread.start();
        Thread.sleep(1000);

        TestUtils.updateRouteWithValidation(managerHelixClusterName, routeForHelix, instanceId, helixAdmin, "ONLINE");
        TestUtils.updateTopicWithValidation(controllerHelixClusterName, topicName, Arrays.asList(0, 1),
                Arrays.asList("0"), helixAdmin, "ONLINE");

        TestUtils.produceMessages(srcBootstrapServer, topicName, 20, numberOfPartitions, isSrcSecure);
        List<ConsumerRecord<Byte[], Byte[]>> records = TestUtils
                .consumeMessage(dstBootstrapServer, topicName, 6000, isDstSecure);
        Assert.assertEquals(records.size(), 20);

        TestUtils.updateRouteWithValidation(managerHelixClusterName, routeForHelix, instanceId, helixAdmin, "OFFLINE");
        runnable.shutdown();
        workerThread.join(5000);
    }


    @AfterClass
    public void shutdown() {
        LOGGER.info("Run after tests");
        srcKafka.shutdown();
        srcKafkaSec.shutdown();
        if (dstKafka != null) {
            dstKafka.shutdown();
            dstKafkaSec.shutdown();
        }
        ZkStarter.stopLocalZkServer();
        org.apache.commons.io.FileUtils.deleteQuietly(new File("kafka.client.truststore.jks"));
        org.apache.commons.io.FileUtils.deleteQuietly(new File("kafka.server.keystore.jks"));
        org.apache.commons.io.FileUtils.deleteQuietly(new File("kafka.server.truststore.jks"));
    }
}
