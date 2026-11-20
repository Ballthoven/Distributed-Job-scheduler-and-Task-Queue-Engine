package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.Job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.JobStatus;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.LeaseLostException;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private Job newJob(int maxAttempts) {
        return Job.create("email", "{}", 0, NOW, maxAttempts, null, NOW);
    }

    @Test
    void newJobIsPendingWithNoAttempts() {
        Job job = newJob(3);

        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(job.getAttempts()).isZero();
        assertThat(job.getLockedBy()).isNull();
    }

    @Test
    void claimingMovesToRunningAndRecordsTheLease() {
        Job job = newJob(3);

        job.markRunning("worker-1", NOW.plusSeconds(30), NOW);

        assertThat(job.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(job.getAttempts()).isEqualTo(1);
        assertThat(job.getLockedBy()).isEqualTo("worker-1");
        assertThat(job.getLockedUntil()).isEqualTo(NOW.plusSeconds(30));
    }

    @Test
    void cannotClaimAJobThatIsAlreadyRunning() {
        Job job = newJob(3);
        job.markRunning("worker-1", NOW.plusSeconds(30), NOW);

        assertThatThrownBy(() -> job.markRunning("worker-2", NOW.plusSeconds(30), NOW))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void successClearsTheLease() {
        Job job = newJob(3);
        job.markRunning("worker-1", NOW.plusSeconds(30), NOW);

        job.markSucceeded("worker-1", NOW.plusSeconds(5));

        assertThat(job.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(job.getLockedBy()).isNull();
        assertThat(job.getLockedUntil()).isNull();
    }

    @Test
    void aWorkerThatDoesNotHoldTheLeaseCannotFinishTheJob() {
        Job job = newJob(3);
        job.markRunning("worker-1", NOW.plusSeconds(30), NOW);

        assertThatThrownBy(() -> job.markSucceeded("worker-2", NOW))
                .isInstanceOf(LeaseLostException.class);
        assertThatThrownBy(() -> job.markFailed("worker-2", "boom", NOW, NOW))
                .isInstanceOf(LeaseLostException.class);
        assertThat(job.getStatus()).isEqualTo(JobStatus.RUNNING);
    }

    @Test
    void cannotFinishAJobThatIsNotRunning() {
        Job job = newJob(3);

        assertThatThrownBy(() -> job.markSucceeded("worker-1", NOW))
                .isInstanceOf(LeaseLostException.class);
    }

    @Test
    void failureWithAttemptsLeftGoesBackToPendingAtTheRetryTime() {
        Job job = newJob(3);
        job.markRunning("worker-1", NOW.plusSeconds(30), NOW);
        Instant retryAt = NOW.plusSeconds(60);

        job.markFailed("worker-1", "boom", retryAt, NOW);

        assertThat(job.getStatus()).isEqualTo(JobStatus.PENDING);
        assertThat(job.getRunAt()).isEqualTo(retryAt);
        assertThat(job.getLastError()).isEqualTo("boom");
        assertThat(job.getLockedBy()).isNull();
    }

    @Test
    void failureOnTheLastAttemptIsTerminal() {
        Job job = newJob(1);
        job.markRunning("worker-1", NOW.plusSeconds(30), NOW);

        job.markFailed("worker-1", "boom", NOW.plusSeconds(60), NOW);

        assertThat(job.getStatus()).isEqualTo(JobStatus.FAILED);
        assertThat(job.getLastError()).isEqualTo("boom");
    }
}
