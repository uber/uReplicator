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
package com.uber.stream.kafka.mirrormaker.manager.utils;
public class ControllerUtils {
    private static final String SEPARATOR = "@";
    public static String getPipelineName(String srcCluster, String dstCluster) {
        return SEPARATOR + srcCluster + SEPARATOR + dstCluster;
    }
    public static boolean isPipelineName(String maybeTopicName) {
        return maybeTopicName.startsWith(SEPARATOR);
    }

    public static String getRouteName(String pipeline, int routeId){
        return pipeline + SEPARATOR + routeId;
    }
}
