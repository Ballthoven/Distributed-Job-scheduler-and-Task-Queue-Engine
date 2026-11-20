package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;


@ConfigurationProperties(prefix = "jobforge.worker")
public record WorkerProperties(
        @DefaultValue("8") int threads,
        @DefaultValue("64") int queueCapacity,
        @DefaultValue("30s") Duration lease,
        @DefaultValue("30s") Duration shutdownTimeout) {
}