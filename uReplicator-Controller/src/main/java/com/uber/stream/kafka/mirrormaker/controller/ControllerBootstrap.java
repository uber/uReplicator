package com.uber.stream.kafka.mirrormaker.controller;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class ControllerBootstrap {

    public static void main(String[] args) throws Exception{

        for(String arg : args){
            System.out.println(arg);
        }

        List<String> argList = new ArrayList<>();
        argList.add("-config");
        argList.add("config/clusters.properties");
        argList.add("-srcClusters");
        argList.add("cluster1,cluster2");
        argList.add("-destClusters");
        argList.add("cluster2,cluster1");
        argList.add("-enableFederated");
        argList.add("true");
        argList.add("-zookeeper");
        argList.add("localhost:2181");
//        argList.add("-helixClusterName");
//        argList.add("9800");
        argList.add("-deploymentName");
        argList.add("test-helix-with-manager");
        argList.add("-env");
        argList.add("dc1.testing-dc1");
        argList.add("-port");
        argList.add("9999");
        argList.add("-instanceId");
        argList.add("controller1");

        args = argList.toArray(new String[]{});

        try {
            ControllerStarter.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
