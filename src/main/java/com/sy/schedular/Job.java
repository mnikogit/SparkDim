package com.sy.schedular;

public class Job {
    Integer jobId;
    Stage finalStage;

    public Integer getJobId() {
        return jobId;
    }

    public void setJobId(Integer jobId) {
        this.jobId = jobId;
    }

    public Stage getFinalStage() {
        return finalStage;
    }

    public void setFinalStage(Stage finalStage) {
        this.finalStage = finalStage;
    }

    public Integer getNumPartitions() {
        if (finalStage instanceof ShuffleMapStage) {
            return finalStage.rdd.partitions.size();
        } else if (finalStage instanceof ResultStage) {
            return finalStage.numPartitions;
        }
        return 0;
    }

}
