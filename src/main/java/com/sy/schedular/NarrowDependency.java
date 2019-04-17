package com.sy.schedular;

import java.util.ArrayList;
import java.util.List;

public class NarrowDependency extends Dependency {

    public List<Integer> getParents(Integer partitionId) {
        List<Integer> integers = new ArrayList<>(1);
        integers.add(partitionId);
        return integers;
    }
}
