package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.job;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.IntegrationTestBase;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
@TestPropertySource(properties = {
        "jobforge.poller.enabled=true",
        "jobforge.poller.interval=PT0.2S"
})
class JobEndToEndIT extends IntegrationTestBase {

    @Autowired
    JobService jobService;
    @Autowired
    JobRepository jobRepository;

    @BeforeEach
    void cleanTable() {
        jobRepository.deleteAll();
    }

    @Test
    void emailJobIsPickedUpAndSucceeds() {
        Job job = jobService.submit(SubmitJobCommand.of("email", "hello"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Job stored = jobRepository.findById(job.getId()).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(stored.getAttempts()).isEqualTo(1);
        });
    }

    @Test
    void flakyJobIsRetriedWithBackoffUntilItSucceeds() {
        Job job = jobService.submit(SubmitJobCommand.of("flaky", "demo"));

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            Job stored = jobRepository.findById(job.getId()).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(stored.getAttempts()).isEqualTo(3);
        });
    }

    @Test
    void jobWithNoHandlerEndsUpFailedAfterAllAttempts() {
        Job job = jobService.submit(SubmitJobCommand.of("does-not-exist", "x").withMaxAttempts(2));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            Job stored = jobRepository.findById(job.getId()).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(JobStatus.FAILED);
            assertThat(stored.getAttempts()).isEqualTo(2);
            assertThat(stored.getLastError()).contains("No handler registered");
        });
    }
}
