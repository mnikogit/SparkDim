package com.sy.schedular;

import java.util.List;

public class TaskScheduler {
    public void submitTasks(TaskSet taskSet) {
        List<Task> tasks = taskSet.getTasks();
        synchronized (this) {
            //下面涉及到task的调度,以及远处调用，本质上，也就是把TASK传输到执行节点进程中
            //FIFO和FAIR
        }
    }
}
