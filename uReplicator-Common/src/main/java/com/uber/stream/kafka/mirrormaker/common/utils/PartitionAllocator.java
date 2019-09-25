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
package com.uber.stream.kafka.mirrormaker.common.utils;

import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: Tboy
 */
public class PartitionAllocator {

    public static List<Integer> allocate(InstanceTopicPartitionHolder currentInstance, List<Integer> partitionAll,
                                         List<InstanceTopicPartitionHolder> instanceAll) {
        if (currentInstance == null) {
            throw new IllegalArgumentException("currentInstance is empty");
        }
        if (partitionAll == null || partitionAll.isEmpty()) {
            throw new IllegalArgumentException("partitionAll is null or mqAll empty");
        }
        if (instanceAll == null || instanceAll.isEmpty()) {
            throw new IllegalArgumentException("instanceAll is null or cidAll empty");
        }
        if (!instanceAll.contains(currentInstance)) {
            throw new IllegalArgumentException(String.format("currentInstance : %s not in instanceAll : %s", currentInstance, instanceAll));
        }
        final List<Integer> result = new ArrayList<>();

        int index = instanceAll.indexOf(currentInstance);
        int mod = partitionAll.size() % instanceAll.size();
        int averageSize =
                partitionAll.size() <= instanceAll.size() ? 1 : (mod > 0 && index < mod ? partitionAll.size() / instanceAll.size()
                        + 1 : partitionAll.size() / instanceAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, partitionAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(partitionAll.get((startIndex + i) % partitionAll.size()));
        }
        return result;
    }

    public static String partitionToString(List<Integer> partitions){
        StringBuilder builder = new StringBuilder(20);
        for(int i = 0; i < partitions.size(); i++){
            builder.append(partitions.get(i));
            if(i < partitions.size() - 1){
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public static List<Integer> partitionToList(String partitions){
        String[] partitionArray = partitions.split(",");
        final Set<Integer> partitionSet = new HashSet<>(partitionArray.length);
        for(String partition : partitionArray){
            if(StringUtils.isNotBlank(partition.trim())){
                partitionSet.add(Integer.parseInt(partition.trim()));
            }
        }
        return new ArrayList<>(partitionSet);
    }
}
