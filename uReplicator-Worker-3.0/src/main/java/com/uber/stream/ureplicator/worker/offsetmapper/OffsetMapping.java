package com.uber.stream.ureplicator.worker.offsetmapper;

public class OffsetMapping {

    private String srcTopic;
    private String dstTopic;
    private int srcPartition;

    public String getSrcTopic() {
        return srcTopic;
    }

    public String getDstTopic() {
        return dstTopic;
    }

    public int getSrcPartition() {
        return srcPartition;
    }

    public int getDstPartition() {
        return dstPartition;
    }

    public long getSrcOffset() {
        return srcOffset;
    }

    @Override
    public String toString() {
        return "OffsetMapping{" +
                "srcTopic='" + srcTopic + '\'' +
                ", dstTopic='" + dstTopic + '\'' +
                ", srcPartition=" + srcPartition +
                ", dstPartition=" + dstPartition +
                ", srcOffset=" + srcOffset +
                ", dstOffset=" + dstOffset +
                '}';
    }

    public long getDstOffset() {
        return dstOffset;
    }

    private int dstPartition;
    private long srcOffset;
    private long dstOffset;

    public OffsetMapping(String srcTopic, String dstTopic, int srcPartition, int dstPartition, long srcOffset, long dstOffset){
        this.srcTopic = srcTopic;
        this.dstTopic = dstTopic;
        this.srcPartition = srcPartition;
        this.dstPartition = dstPartition;
        this.srcOffset = srcOffset;
        this.dstOffset = dstOffset;
    }
}
