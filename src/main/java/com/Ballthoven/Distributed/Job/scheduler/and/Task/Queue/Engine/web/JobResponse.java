package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.web;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.Job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.JobStatus;

import java.time.Instant;
import java.util.UUID;

public record JobResponse(UUID id,
                          String type,
                          String payload,
                          JobStatus status,
                          int priority,
                          Instant runAt,
                          int attempts,
                          int maxAttempts,
                          String idempotencyKey,
                          String lockedBy,
                          Instant lockedUntil,
                          String lastError,
                          Instant createdAt,
                          Instant updatedAt) {
    public static JobResponse from(Job job){
        return new JobResponse(
                job.getId(),
                job.getType(),
                job.getPayload(),
                job.getStatus(),
                job.getPriority(),
                job.getRunAt(),
                job.getAttempts(),
                job.getMaxAttempts(),
                job.getIdempotencyKey(),
                job.getLockedBy(),
                job.getLockedUntil(),
                job.getLastError(),
                job.getCreatedAt(),
                job.getUpdatedAt());
    }
}
