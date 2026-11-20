package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "jobs", indexes = {
        @Index(name = "idx_jobs_claimable", columnList = "status, priority DESC, run_at"),
        @Index(name = "idx_jobs_running_lease", columnList = "status, locked_until")
})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString(onlyExplicitlyIncluded = true)

public class Job {
    @Id
    @ToString.Include
    private UUID id;

    @ToString.Include
    @Column(nullable = false, length = 100)
    private String type;

    @Column(columnDefinition = "text")
    private String payload;

    @ToString.Include
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private JobStatus status;

    @Column(nullable = false)
    private int priority;

    @Column(name = "run_at", nullable = false)
    private Instant runAt;

    @ToString.Include
    @Column(nullable = false)
    private int attempts;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;
 @Column(name = "idempotency_key", length = 200, unique = true)
    private String idempotencyKey;

    @Column(name = "locked_by", length = 100)
    private String lockedBy;

    @Column(name = "locked_until")
    private Instant lockedUntil;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @Version
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public static Job create(String type, String payload, int priority, Instant runAt,
                             int maxAttempts, String idempotencyKey, Instant now) {
        Job job = new Job();
        job.id = UUID.randomUUID();
        job.type = type;
        job.payload = payload;
        job.status = JobStatus.PENDING;
        job.priority = priority;
        job.runAt = runAt;
        job.attempts = 0;
        job.maxAttempts = maxAttempts;
        job.idempotencyKey = idempotencyKey;
        job.createdAt = now;
        job.updatedAt = now;
        return job;
    }

    public void markRunning(String workerId, Instant leaseUntil, Instant now) {
        requireStatus(JobStatus.PENDING, "claim");
        this.status = JobStatus.RUNNING;
        this.lockedBy = workerId;
        this.lockedUntil = leaseUntil;
        this.attempts++;
        this.updatedAt = now;
    }

    public void markSucceeded(String workerId, Instant now) {
        requireLease(workerId);
        this.status = JobStatus.SUCCEEDED;
        this.lockedBy = null;
        this.lockedUntil = null;
        this.lastError = null;
        this.updatedAt = now;
    }


    public void markFailed(String workerId, String error, Instant retryAt, Instant now) {
        requireLease(workerId);
        this.lastError = error;
        this.lockedBy = null;
        this.lockedUntil = null;
        this.updatedAt = now;
        if (attempts >= maxAttempts) {
            this.status = JobStatus.FAILED;
        } else {
            this.status = JobStatus.PENDING;
            this.runAt = retryAt;
        }
    }

    private void requireStatus(JobStatus expected, String action) {
        if (status != expected) {
            throw new IllegalStateException(
                    "Cannot " + action + " job " + id + ": expected " + expected + " but was " + status);
        }
    }

    private void requireLease(String workerId) {
        if (status != JobStatus.RUNNING || lockedBy == null || !lockedBy.equals(workerId)) {
            throw new LeaseLostException(id, workerId);
        }
    }
}
