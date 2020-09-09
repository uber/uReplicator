package com.uber.stream.ureplicator.worker.offsetmapper;

public interface OffsetMapper {

    void mapOffset(String srcTopic, int srcPartition, long srcOffset, String dstTopic, int dstPartition, long dstOffset);
    void close();
}
