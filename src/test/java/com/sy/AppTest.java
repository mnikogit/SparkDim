package com.sy;

import static org.junit.Assert.assertTrue;

import com.sy.schedular.DagSchedular;
import com.sy.schedular.NarrowDependency;
import com.sy.schedular.RDD;
import com.sy.schedular.ShuffleDependency;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * flatmap -> map -> reduceBykey -> collect
     */
    @Test
    public void testJob1()

    {
        DagSchedular dagSchedular = new DagSchedular();

        RDD flatmaprdd = new RDD();
        flatmaprdd.setName("flatmapRdd_0");

        RDD maprdd = new RDD();
        maprdd.setName("mapRdd_1");
        NarrowDependency narrowDependency = new NarrowDependency();
        narrowDependency.setRdd(flatmaprdd);
        maprdd.setDepedencies(narrowDependency);

        RDD reduceByKeyRdd = new RDD();
        reduceByKeyRdd.setName("reduceByKeyRdd_2");
        NarrowDependency narrowDependency2 = new NarrowDependency();
        narrowDependency2.setRdd(maprdd);
        reduceByKeyRdd.setDepedencies(narrowDependency2);

        RDD actionRdd = new RDD();
        actionRdd.setName("CollectRdd_3");
        ShuffleDependency shuffleDependency = new ShuffleDependency();
        shuffleDependency.setRdd(reduceByKeyRdd);
        shuffleDependency.setShuffleId(0);
        actionRdd.setDepedencies(shuffleDependency);


        dagSchedular.submitJob(actionRdd);

    }

    /**
     * map-> groupby
     * \
     * join-> saveAs
     * /
     * flatmap
     */
    @Test
    public void testJob2()

    {
        DagSchedular dagSchedular = new DagSchedular();

        RDD map0 = new RDD();
        map0.setName("mapRdd_0");

        RDD groupbykeyRdd = new RDD();
        groupbykeyRdd.setName("groupByKeyRdd_1");
        ShuffleDependency shuffleDependency = new ShuffleDependency();
        shuffleDependency.setRdd(map0);
        shuffleDependency.setShuffleId(0);
        groupbykeyRdd.setDepedencies(shuffleDependency);

        RDD flatmap = new RDD();
        flatmap.setName("flatmapRdd_2");

        RDD joinRdd = new RDD();
        joinRdd.setName("joinRdd_3");

        ShuffleDependency shuffleDependency1 = new ShuffleDependency();
        shuffleDependency1.setRdd(flatmap);
        shuffleDependency1.setShuffleId(1);
        joinRdd.setDepedencies(shuffleDependency1);

        ShuffleDependency shuffleDependency2 = new ShuffleDependency();
        shuffleDependency2.setRdd(groupbykeyRdd);
        shuffleDependency2.setShuffleId(2);
        joinRdd.setDepedencies(shuffleDependency2);


        RDD actionRdd = new RDD();
        actionRdd.setName("saveAs_3");
        NarrowDependency narrowDependency = new NarrowDependency();
        narrowDependency.setRdd(joinRdd);
        actionRdd.setDepedencies(narrowDependency);

        dagSchedular.submitJob(actionRdd);

    }

}
