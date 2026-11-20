package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.web;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.Job;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.JobService;
import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model.SubmitJobCommand;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobDriver {

    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 200;

    private final JobService jobs;

    @PostMapping
    public ResponseEntity<JobResponse> submit(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody SubmitJobRequest request) {

        String key = (idempotencyKey == null || idempotencyKey.isBlank()) ? null : idempotencyKey.trim();
        if (key != null && key.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            log.warn("Rejected submit of type '{}': Idempotency-Key longer than {} characters",
                    request.type(), MAX_IDEMPOTENCY_KEY_LENGTH);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Idempotency-Key must be at most " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }

        SubmitJobCommand command = new SubmitJobCommand(
                request.type(),
                request.payload(),
                request.priority() != null ? request.priority() : 0,
                request.runAt(),
                request.maxAttempts() != null ? request.maxAttempts() : SubmitJobCommand.DEFAULT_MAX_ATTEMPTS,
                key);

        Job job = jobs.submit(command);
        return ResponseEntity.accepted()
                .location(URI.create("/api/jobs/" + job.getId()))
                .body(JobResponse.from(job));
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable UUID id) {
        return jobs.find(id)
                .map(JobResponse::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Job " + id + " not found"));
    }

}
