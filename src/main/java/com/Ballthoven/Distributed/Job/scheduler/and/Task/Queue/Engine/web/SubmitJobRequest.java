package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.web;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public record SubmitJobRequest(@NotBlank @Size(max = 100) String type,
                               String payload,
                               @Min(0) @Max(100) Integer priority,
                               Instant runAt,
                               @Min(1) @Max(20) Integer maxAttempts) {

}
