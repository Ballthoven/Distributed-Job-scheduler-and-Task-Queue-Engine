package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.Job;

public interface JobHandler {
    String type();
    void handle(Job job) throws Exception;
}
