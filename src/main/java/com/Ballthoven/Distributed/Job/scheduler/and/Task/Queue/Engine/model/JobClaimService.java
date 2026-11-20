package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobClaimService {

    private final JobRepository jobs;
    private final Clock clock;


    @Transactional
    public List<Job> claimBatch(String workerId, int limit, Duration lease) {
        if (limit < 1) {
            return List.of();
        }
        Instant now = clock.instant();
        List<Job> claimed = jobs.lockNextBatch(JobStatus.PENDING, now, PageRequest.of(0, limit));
        Instant leaseUntil = now.plus(lease);
        for (Job job : claimed) {
            job.markRunning(workerId, leaseUntil, now);
        }
        if (!claimed.isEmpty()) {
            log.debug("Worker {} claimed {} job(s), leased until {}", workerId, claimed.size(), leaseUntil);
        }
        return claimed;
    }
  @Transactional
    public int reclaimExpiredLeases() {
        return jobs.reclaimExpiredLeases(clock.instant());
    }
}