package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.IntegrationTestBase;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class JobConcurrencyIT extends IntegrationTestBase {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    JobService jobService;
    @Autowired
    JobClaimService claimService;
    @Autowired
    JobRepository jobRepository;

    @BeforeEach
    void cleanTable() {
        jobRepository.deleteAll();
    }

    /**
     * Runs every task on its own thread, all released at the same instant by a barrier.
     */
    private <T> List<T> fireSimultaneously(int threads, java.util.function.IntFunction<Callable<T>> taskFactory)
            throws Exception {
        CyclicBarrier startLine = new CyclicBarrier(threads);
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                Callable<T> task = taskFactory.apply(i);
                futures.add(executor.submit(() -> {
                    startLine.await(30, TimeUnit.SECONDS);
                    return task.call();
                }));
            }
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get(60, TimeUnit.SECONDS));
            }
            return results;
        }
    }

    @Test
    void oneJobContendedBy1000ThreadsIsClaimedExactlyOnce() throws Exception {
        Job job = jobService.submit(SubmitJobCommand.of("noop", "{}"));

        List<List<Job>> results = fireSimultaneously(1000,
                i -> () -> claimService.claimBatch("worker-" + i, 1, LEASE));

        List<Job> winners = results.stream().flatMap(List::stream).toList();
        assertThat(winners).hasSize(1);
        assertThat(winners.getFirst().getId()).isEqualTo(job.getId());

        Job stored = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(stored.getAttempts()).isEqualTo(1);
        assertThat(stored.getLockedBy()).isEqualTo(winners.getFirst().getLockedBy());
    }

    @Test
    void manyWorkersDrainTheQueueWithNoDuplicatesAndNoLostJobs() throws Exception {
        int jobCount = 500;
        Instant now = Instant.now();
        List<Job> seeded = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            seeded.add(Job.create("noop", "{}", i % 5, now.minusSeconds(1), 5, null, now));
        }
        jobRepository.saveAll(seeded);

        List<List<UUID>> perWorker = fireSimultaneously(40, worker -> () -> {
            List<UUID> mine = new ArrayList<>();
            while (true) {
                List<Job> batch = claimService.claimBatch("worker-" + worker, 7, LEASE);
                if (batch.isEmpty()) {
                    return mine;
                }
                batch.forEach(job -> mine.add(job.getId()));
            }
        });

        List<UUID> allClaims = perWorker.stream().flatMap(List::stream).toList();
        Set<UUID> distinct = new HashSet<>(allClaims);
        assertThat(allClaims).as("a job was handed to two workers").hasSize(distinct.size());
        assertThat(distinct).as("a job was lost").hasSize(jobCount);

        List<Job> stored = jobRepository.findAll();
        assertThat(stored).allSatisfy(job -> {
            assertThat(job.getStatus()).isEqualTo(JobStatus.RUNNING);
            assertThat(job.getAttempts()).isEqualTo(1);
        });
    }

    @Test
    void concurrentSubmitsWithTheSameIdempotencyKeyCreateOneJob() throws Exception {
        SubmitJobCommand command = SubmitJobCommand.of("noop", "{}").withIdempotencyKey("order-42");

        List<UUID> ids = fireSimultaneously(200, i -> () -> jobService.submit(command).getId());

        assertThat(new HashSet<>(ids)).hasSize(1);
        assertThat(jobRepository.count()).isEqualTo(1);
    }

    @Test
    void highestPriorityJobIsClaimedFirstAndFutureJobsAreLeftAlone() {
        Instant now = Instant.now();
        jobService.submit(SubmitJobCommand.of("low", "{}").withPriority(0).withRunAt(now.minusSeconds(10)));
        Job high = jobService.submit(SubmitJobCommand.of("high", "{}").withPriority(9).withRunAt(now.minusSeconds(1)));
        Job future = jobService.submit(SubmitJobCommand.of("future", "{}").withPriority(9).withRunAt(now.plusSeconds(3600)));

        List<Job> claimed = claimService.claimBatch("worker-1", 1, LEASE);

        assertThat(claimed).extracting(Job::getId).containsExactly(high.getId());
        assertThat(jobRepository.findById(future.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.PENDING);
    }

    @Test
    void expiredLeaseIsRecoveredAndTheOldWorkerIsFencedOut() {
        Job job = jobService.submit(SubmitJobCommand.of("noop", "{}"));

        // worker-A claims with a lease that expires almost immediately, then "dies".
        assertThat(claimService.claimBatch("worker-A", 1, Duration.ofMillis(1))).hasSize(1);

        await().atMost(Duration.ofSeconds(5)).until(() -> claimService.reclaimExpiredLeases() == 1);

        // worker-B takes over.
        List<Job> second = claimService.claimBatch("worker-B", 1, LEASE);
        assertThat(second).extracting(Job::getId).containsExactly(job.getId());

        // worker-A wakes up late and tries to report success: it must be rejected.
        assertThatThrownBy(() -> jobService.complete(job.getId(), "worker-A"))
                .isInstanceOf(LeaseLostException.class);

        jobService.complete(job.getId(), "worker-B");
        Job stored = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(stored.getAttempts()).isEqualTo(2);
    }

    @Test
    void failedJobIsRetriedUntilAttemptsRunOut() {
        Job job = jobService.submit(SubmitJobCommand.of("noop", "{}").withMaxAttempts(2));

        claimService.claimBatch("worker-1", 1, LEASE);
        jobService.fail(job.getId(), "worker-1", "boom", Duration.ZERO);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus()).isEqualTo(JobStatus.PENDING);

        claimService.claimBatch("worker-1", 1, LEASE);
        jobService.fail(job.getId(), "worker-1", "boom again", Duration.ZERO);

        Job stored = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(JobStatus.FAILED);
        assertThat(stored.getAttempts()).isEqualTo(2);
        assertThat(stored.getLastError()).isEqualTo("boom again");
    }
}