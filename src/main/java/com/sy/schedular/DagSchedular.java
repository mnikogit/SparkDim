package com.sy.schedular;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DagSchedular {
    private AtomicInteger nextStageId = new AtomicInteger(0);
    private AtomicInteger nextJobId = new AtomicInteger(0);
    private Map<Integer, Set<Integer>> jobIdtoStageIds = new HashMap<>();//jobid到stageid的映射
    private Map<Integer, Stage> stageIdtoStage = new HashMap<>(); //stageid到stage的映射
    private Map<Integer, ShuffleMapStage> shuffleIdToMapStage = new HashMap<>();


    private Set<Stage> waitingStages = new HashSet<>();
    private Set<Stage> runningStages = new HashSet<>();
    private Set<Stage> failedStages = new HashSet<>();


    private TaskScheduler taskScheduler = new TaskScheduler();

    public void submitJob(RDD rdd) {
        Integer jobId = nextJobId.getAndIncrement();
        Job job = handleJobSubmitted(jobId, rdd);
        submitStage(job.getFinalStage());
    }

    private void submitStage(Stage stage) {
        if ( !waitingStages.contains(stage) && !runningStages.contains(stage) && !failedStages.contains(stage)) {
            List<Stage> missingParentStages = getMissingParentStages(stage);
            if (missingParentStages.isEmpty()) {
                submitMissingTasks(stage);
            } else {
                missingParentStages.forEach(s -> {
                    submitStage(s);
                });
            }
        }
    }

    private void submitMissingTasks(Stage stage) {
        runningStages.add(stage);
        //这里需要记录已经执行的分区
        List<Partition> partitions = stage.rdd.partitions;
        //这里需要根据Stage的类型，生成ShuffleMapTask和ResultTask
        List<Task> tasks = partitions.stream().map(partition -> {
            Task task = new Task();
            task.setStageId(stage.id);
            task.setPartitionId(partition.index);
            task.setJobId(stage.firstJobId);
            return task;
        }).collect(Collectors.toList());

        if (!tasks.isEmpty()) {
            taskScheduler.submitTasks(new TaskSet(tasks, stage.id));
        } else {
           // 这里需要将stage的从运行中状态 进行迁移，spark使用了事件驱动和异步消息，这里不好模拟
        }
    }

    private List<Stage> getMissingParentStages(Stage stage) {
        Set<Stage> missing = new HashSet<>();
        Set<RDD> visited = new HashSet<>();
        Stack<RDD> stack = new Stack<>();
        stack.push(stage.rdd);
        while (!stack.isEmpty()) {
            RDD pop = stack.pop();
            if (!visited.contains(pop)) {
                visited.add(pop);

                pop.depedencies.forEach(dep -> {
                    if (dep instanceof ShuffleDependency) {
                        ShuffleMapStage mapstage = getOrCreateShuffleMapStage((ShuffleDependency) dep, stage.firstJobId);
                        missing.add(mapstage);
                    } else if (dep instanceof NarrowDependency) {
                        stack.push(dep.rdd);
                    }
                });
            }
        }
        return missing.stream().sorted(new Comparator<Stage>() {
            @Override
            public int compare(Stage o1, Stage o2) {
                return o1.id.compareTo(o2.id);
            }
        }).collect(Collectors.toList());
    }


    private Job handleJobSubmitted(Integer jobId, RDD rdd) {
        //stage分两类，导致job划分的resultstage和shuffleMapStage
        //获取最后的一个stage
        ResultStage resultStage = createResultStage(rdd, jobId);
        Job job = new Job();
        job.setJobId(jobId);
        job.setFinalStage(resultStage);
        return job;
    }

    private ResultStage createResultStage(RDD rdd, Integer jobId) {
        //获取父stage
        List<Stage> parentStages = getOrCreateParentStages(rdd, jobId);
        Integer stageId = nextStageId.getAndIncrement();
        ResultStage resultStage = new ResultStage();
        resultStage.setId(stageId);
        resultStage.setRdd(rdd);
        resultStage.setFirstJobId(jobId);
        resultStage.parents.addAll(parentStages);
        stageIdtoStage.put(stageId, resultStage);
        //构造jobIdtoStageIds的映射关系
        updateJobIdStageIdMaps(jobId, resultStage);
        return resultStage;
    }

    private void updateJobIdStageIdMaps(Integer jobId, Stage resultStage) {
        Stack<Stage> stack = new Stack<>();
        stack.push(resultStage);
        Set<Stage> visited = new HashSet<>();
        while (!stack.isEmpty()) {
            Stage pop = stack.pop();
            if (!visited.contains(pop)) {
                visited.add(pop);
                pop.jobIds.add(jobId);
                Set<Integer> integers = jobIdtoStageIds.get(jobId);
                if (integers == null) {
                    Set<Integer> stageIds = new HashSet<>();
                    stageIds.add(pop.id);
                    jobIdtoStageIds.put(jobId, stageIds);
                } else {
                    integers.add(pop.id);
                }
                List<Stage> parentsWithoutThisJobId = pop.parents.stream().filter(
                        new Predicate<Stage>() {
                            @Override
                            public boolean test(Stage stage) {
                                return !stage.jobIds.contains(jobId);
                            }
                        }
                ).collect(Collectors.toList());
                stack.addAll(parentsWithoutThisJobId);
            }
        }
    }

    private List<Stage> getOrCreateParentStages(RDD rdd, Integer jobId) {
        return getShuffleDependencies(rdd).stream().map(i -> {
            return getOrCreateShuffleMapStage(i, jobId);
        }).collect(Collectors.toList());
    }


    private Set<ShuffleDependency> getShuffleDependencies(RDD rdd) {
        Set<ShuffleDependency> parents = new HashSet<>();
        Set<RDD> visited = new HashSet<>();
        Stack<RDD> stack = new Stack<>();
        stack.push(rdd);
        while (!stack.isEmpty()) {
            RDD pop = stack.pop();
            if (!visited.contains(pop)) {
                visited.add(pop);
                pop.depedencies.forEach(i -> {
                    if (i instanceof ShuffleDependency) {
                        parents.add((ShuffleDependency) i);
                    } else {
                        stack.push(i.getRdd());
                    }
                });
            }
        }
        return parents;
    }


    private ShuffleMapStage getOrCreateShuffleMapStage(ShuffleDependency shuffleDependency, Integer jobId) {
        ShuffleMapStage shuffleMapStage = shuffleIdToMapStage.get(shuffleDependency.shuffleId);

        if (shuffleMapStage != null) {
            return shuffleMapStage;
        } else {
//            Set<ShuffleDependency> parents = new HashSet<>();
//            Set<RDD> visited = new HashSet<>();
//            Stack<RDD> stack = new Stack<>();
//            stack.push(shuffleDependency.rdd);
//            while (!stack.isEmpty()) {
//                RDD pop = stack.pop();
//                if (!visited.contains(pop)) {
//                    visited.add(pop);
//                    Set<ShuffleDependency> shuffleDependencies = getShuffleDependencies(pop);
//                    shuffleDependencies.forEach(i -> {
//                        if (!shuffleIdToMapStage.containsKey(i.shuffleId)) {
//                            parents.add(i);
//                            stack.push(i.rdd);
//                        }
//                    });
//                }
//            }
//
//            parents.forEach(dep -> {
//                if (shuffleIdToMapStage.containsKey(dep.shuffleId)) {
//                    createShuffleMapStage(dep, jobId);
//                }
//            });

            return createShuffleMapStage(shuffleDependency, jobId);
        }
    }


    private ShuffleMapStage createShuffleMapStage(ShuffleDependency shuffleDependency, Integer jobId) {
        RDD rdd = shuffleDependency.rdd;
        int id = nextStageId.getAndIncrement();
        List<Stage> parentStages = getOrCreateParentStages(rdd, jobId);

        ShuffleMapStage shuffleMapStage = new ShuffleMapStage();
        shuffleMapStage.setRdd(rdd);
        shuffleMapStage.setId(id);
        shuffleMapStage.setParents(parentStages);
        shuffleMapStage.setFirstJobId(jobId);
        shuffleMapStage.setShuffleDependency(shuffleDependency);

        stageIdtoStage.put(id, shuffleMapStage);
        shuffleIdToMapStage.put(shuffleDependency.shuffleId, shuffleMapStage);

        updateJobIdStageIdMaps(jobId, shuffleMapStage);
        return shuffleMapStage;
    }


}
