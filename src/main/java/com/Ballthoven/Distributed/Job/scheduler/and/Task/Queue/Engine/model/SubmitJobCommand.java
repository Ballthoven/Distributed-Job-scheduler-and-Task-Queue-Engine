package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model;

import java.time.Instant;

public record SubmitJobCommand(String type,
                               String payload,
                               int priority,
                               Instant runAt,
                               int maxAttempts,
                               String idempotencyKey) {

    public static final int DEFAULT_MAX_ATTEMPTS = 5;

    public static SubmitJobCommand of (String type, String payload) {
        return new SubmitJobCommand(type, payload, 0, null, DEFAULT_MAX_ATTEMPTS, null);
    }

    public SubmitJobCommand withPriority(int priority) {
        return new SubmitJobCommand(type, payload, priority, runAt, maxAttempts, idempotencyKey);
    }

    public SubmitJobCommand withRunAt(Instant runAt) {
        return new SubmitJobCommand(type, payload, priority, runAt, maxAttempts, idempotencyKey);
    }

    public SubmitJobCommand withMaxAttempts(int maxAttempts) {
        return new SubmitJobCommand(type, payload, priority, runAt, maxAttempts, idempotencyKey);
    }

    public SubmitJobCommand withIdempotencyKey(String idempotencyKey) {
        return new SubmitJobCommand(type, payload, priority, runAt, maxAttempts, idempotencyKey);
    }

}
