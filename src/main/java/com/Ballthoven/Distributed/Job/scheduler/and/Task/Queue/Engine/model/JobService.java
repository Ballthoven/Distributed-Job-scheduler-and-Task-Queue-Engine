package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model;

import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobService {

    private final JobRepository jobs;
    private final Clock clock;


    public Job submit(SubmitJobCommand cmd) {
        String key = cmd.idempotencyKey();
        if (key != null) {
            Optional<Job> existing = jobs.findByIdempotencyKey(key);
            if (existing.isPresent()) {
                log.debug("Idempotency key '{}' already used by job {}; returning it", key, existing.get().getId());
                return existing.get();
            }
        }

        Instant now = clock.instant();
        Job job = Job.create(cmd.type(), cmd.payload(), cmd.priority(),
                cmd.runAt() != null ? cmd.runAt() : now,
                cmd.maxAttempts(), key, now);
        try {
            Job saved = jobs.saveAndFlush(job);
            log.info("Submitted job {} (type={}, priority={}, idempotencyKey={})",
                    saved.getId(), saved.getType(), saved.getPriority(), key);
            return saved;
        } catch (DataIntegrityViolationException e) {
            if (key == null) {
                throw e;
            }
            log.debug("Lost the idempotency race for key '{}'; returning the winning job", key);
            return jobs.findByIdempotencyKey(key).orElseThrow(() -> e);
        }
    }

    @Transactional
    public void complete(UUID jobId, String workerId) {
        Job job = jobs.findById(jobId).orElseThrow(() -> new LeaseLostException(jobId, workerId));
        job.markSucceeded(workerId, clock.instant());
        log.debug("Job {} succeeded on attempt {} (worker {})", jobId, job.getAttempts(), workerId);
    }

    @Transactional
    public void fail(UUID jobId, String workerId, String error, Duration retryDelay) {
        Job job = jobs.findById(jobId).orElseThrow(() -> new LeaseLostException(jobId, workerId));
        Instant now = clock.instant();
        job.markFailed(workerId, error, now.plus(retryDelay), now);
        log.warn("Job {} failed on attempt {}/{} and is now {} (worker {}): {}",
                jobId, job.getAttempts(), job.getMaxAttempts(), job.getStatus(), workerId, error);
    }

    @Transactional(readOnly = true)
    public Optional<Job> find(UUID jobId) {
        return jobs.findById(jobId);
    }
}
