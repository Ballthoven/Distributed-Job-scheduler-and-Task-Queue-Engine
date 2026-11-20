package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model;

import java.util.UUID;

public class LeaseLostException extends RuntimeException {
public LeaseLostException(UUID jobId, String workerId) {
    super("Worker '" + workerId + "' does not hold the lease on job " + jobId);
        }
}
