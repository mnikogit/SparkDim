package com.sy.schedular;

import java.util.*;

public abstract class Stage {
    Integer firstJobId;
    Integer id;
    RDD rdd;
    Integer numTasks;
    Set<Stage> parents = new HashSet<>();
    Set<Integer> jobIds = new HashSet<>();
    Integer numPartitions = rdd != null ? rdd.partitions.size() : 0;

    public void setFirstJobId(Integer firstJobId) {
        this.firstJobId = firstJobId;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setRdd(RDD rdd) {
        this.rdd = rdd;
    }

    public void setNumTasks(Integer numTasks) {
        this.numTasks = numTasks;
    }

    public List<Stage> getParents() {
        return new ArrayList<>(parents);
    }

    public void setParents(List<Stage> parents) {
        this.parents.addAll(parents);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Stage stage = (Stage) o;
        return Objects.equals(id, stage.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    abstract List<Integer> findMissingPartitions();


}

class ResultStage extends Stage {

    @Override
    List<Integer> findMissingPartitions() {
        return null;
    }
}


class ShuffleMapStage extends Stage {

    ShuffleDependency shuffleDependency;

    public ShuffleDependency getShuffleDependency() {
        return shuffleDependency;
    }

    public void setShuffleDependency(ShuffleDependency shuffleDependency) {
        this.shuffleDependency = shuffleDependency;
    }

    @Override
    List<Integer> findMissingPartitions() {
        return null;
    }
}