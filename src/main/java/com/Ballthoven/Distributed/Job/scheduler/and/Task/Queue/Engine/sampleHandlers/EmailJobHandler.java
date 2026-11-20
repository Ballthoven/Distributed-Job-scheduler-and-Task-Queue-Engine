package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.sampleHandlers;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.Job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker.JobHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EmailJobHandler implements JobHandler {
    @Override
    public String type() {
        return "email";
        }

    @Override
    public void handle(Job job) throws Exception {
        log.info("Sending email for job {} (attempt {}): {}", job.getId(), job.getAttempts(), job.getPayload());
        Thread.sleep(200);
    }

    }
