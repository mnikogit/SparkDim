package com.sy.schedular;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RDD {

    String name;

    List<Dependency> depedencies = new ArrayList<>();

    List<Partition> partitions = new ArrayList<>();

    public List<Dependency> getDependencies() {
        return depedencies;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Dependency> getDepedencies() {
        return depedencies;
    }

    public void setDepedencies(List<Dependency> depedencies) {
        this.depedencies.addAll(depedencies);
    }

    public void setDepedencies(Dependency depedency) {
        this.depedencies.add(depedency);
    }

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RDD rdd = (RDD) o;
        return Objects.equals(name, rdd.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "RDD{" +
                "name='" + name + '\'' +
                '}';
    }
}
