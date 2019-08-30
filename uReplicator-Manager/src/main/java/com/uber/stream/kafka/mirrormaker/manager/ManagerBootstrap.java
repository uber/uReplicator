package com.uber.stream.kafka.mirrormaker.manager;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class ManagerBootstrap {

    public static void main(String[] args) throws Exception{

        for(String arg : args){
            System.out.println(arg);
        }

        List<String> argList = new ArrayList<>();
        argList.add("-config");
        argList.add("config/clusters.properties");
        argList.add("-srcClusters");
        argList.add("cluster1");
        argList.add("-destClusters");
        argList.add("cluster2");
        argList.add("-enableRebalance");
        argList.add("true");
        argList.add("-zookeeper");
        argList.add("localhost:2181");
        argList.add("-managerPort");
        argList.add("9500");
        argList.add("-deployment");
        argList.add("test-helix-with-manager");
        argList.add("-env");
        argList.add("dc1.testing-dc1");
//        argList.add("-instanceId");
//        argList.add(InetAddress.getLocalHost().getHostName());
//        argList.add("-controllerPort");
//        argList.add("9999");
//        argList.add("-graphiteHost");
//        argList.add("127.0.0.1");
//        argList.add("-graphitePort");
//        argList.add("4756");
        argList.add("-metricsPrefix");
        argList.add("ureplicator-manager");
        argList.add("-workloadRefreshPeriodInSeconds");
        argList.add("300");
        argList.add("-initMaxNumPartitionsPerRoute");
        argList.add("1500");
        argList.add("-maxNumPartitionsPerRoute");
        argList.add("2000");
        argList.add("-initMaxNumWorkersPerRoute");
        argList.add("10");
        argList.add("-maxNumWorkersPerRoute");
        argList.add("80");
        args = argList.toArray(new String[]{});

        try {
            ManagerStarter.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
