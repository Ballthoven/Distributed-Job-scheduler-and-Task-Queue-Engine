package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.sampleHandlers;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.Job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker.JobHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class FlakyJobHandler implements JobHandler {
    @Override
    public String type() {
        return "flaky";
    }

    @Override
    public void handle(Job job) {
        if (job.getAttempts() < 3) {
            log.warn("Job {} failing on purpose (attempt {})", job.getId(), job.getAttempts());
            throw new IllegalStateException("Simulated failure on attempt " + job.getAttempts());
        }
        log.info("Job {} succeeded on attempt {}", job.getId(), job.getAttempts());
    }
}
