package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.configuration.WorkerProperties;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.Job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.JobClaimService;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.JobService;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.LeaseLostException;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "jobforge.poller.enabled", havingValue = "true", matchIfMissing = true)
public class JobPoller {

    private static final Duration RETRY_BASE = Duration.ofSeconds(2);
    private static final Duration RETRY_CAP = Duration.ofMinutes(5);

    private final JobClaimService claims;
    private final JobService jobs;
    private final JobHandlerRegistry handlers;
    private final BoundedWorkerPool pool;
    private final WorkerProperties props;

     private final String workerId = "node-" + UUID.randomUUID().toString().substring(0, 8);

    @PostConstruct
    void announce() {
        log.info("Job poller started as {}", workerId);
    }

    @Scheduled(fixedDelayString = "${jobforge.poller.interval:PT0.5S}")
    public void poll() {
        int reclaimed = claims.reclaimExpiredLeases();
        if (reclaimed > 0) {
            log.warn("Reclaimed {} job(s) with expired leases", reclaimed);
        }

        int free = pool.remainingCapacity();
        if (free <= 0) {
            return;
        }

        List<Job> claimed = claims.claimBatch(workerId, free, props.lease());
        for (Job job : claimed) {
            if (!pool.trySubmit(() -> execute(job))) {
                 log.warn("Worker pool rejected claimed job {}; it will be recovered when its lease expires", job.getId());
            }
        }
    }

    private void execute(Job job) {
        try {
            JobHandler handler = handlers.find(job.getType()).orElse(null);
            if (handler == null) {
                jobs.fail(job.getId(), workerId, "No handler registered for type '" + job.getType() + "'", retryDelay(job));
                return;
            }
            log.debug("Running {}", job);
            handler.handle(job);
            jobs.complete(job.getId(), workerId);
        } catch (LeaseLostException lost) {
            log.warn("Lost the lease on job {} before finishing: {}", job.getId(), lost.getMessage());
        } catch (Exception failure) {
            try {
                jobs.fail(job.getId(), workerId, String.valueOf(failure), retryDelay(job));
            } catch (LeaseLostException lost) {
                log.warn("Lost the lease on job {} while reporting failure", job.getId());
            }
        }
    }

    private Duration retryDelay(Job job) {
        return BackOff.fullJitter(job.getAttempts(), RETRY_BASE, RETRY_CAP, ThreadLocalRandom.current());
    }
}
