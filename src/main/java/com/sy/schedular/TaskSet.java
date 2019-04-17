package com.sy.schedular;

import java.util.List;

public class TaskSet {
    private List<Task> tasks;
    private Integer stageId;
    private Integer priority;

    public TaskSet(List<Task> tasks, Integer stageId) {
        this.tasks = tasks;
        this.stageId = stageId;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }
}
