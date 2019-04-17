package com.sy.schedular;

import java.util.ArrayList;
import java.util.List;

public abstract class Dependency {
    RDD rdd;

    public RDD getRdd() {
        return rdd;
    }

    public void setRdd(RDD rdd) {
        this.rdd = rdd;
    }
}


