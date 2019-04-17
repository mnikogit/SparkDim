package com.sy.schedular;


/*
    一个partition对应一个task
 */
public class Task {
    private Integer stageId;
    private Integer partitionId;
    private Integer JobId;

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Integer getJobId() {
        return JobId;
    }

    public void setJobId(Integer jobId) {
        JobId = jobId;
    }
}
