# Distributed Job Scheduler & Task Queue Engine

A job scheduler and task queue built to be **correct under concurrency**. Many workers pull jobs from one PostgreSQL table, and the system guarantees that a job is never handed to two workers at once, that a crashed worker's job is recovered, and that retrying a client request never creates a duplicate job. Those guarantees are proven by tests that run 1000 competing threads against a real database.

**Stack:** Java 21 · Spring Boot 3.5 · Spring Data JPA / Hibernate · PostgreSQL 16 · Redis (Redisson) · Kafka · Docker · Testcontainers · JUnit 5 · AssertJ · Awaitility

---

## How it works

    client ──POST /api/jobs──▶ JobController ──▶ JobService.submit ──▶ [ jobs table ]
                                                                            ▲
        JobPoller (every 0.5s)                                              │
          1. reclaim expired leases ────────────────────────────────────────┤
          2. claim up to N jobs (SELECT ... FOR UPDATE SKIP LOCKED) ────────┤
          3. hand each job to the BoundedWorkerPool                         │
                 └─▶ JobHandler (by job type) ──▶ complete / fail+backoff ──┘

### Job lifecycle

    PENDING --claim--> RUNNING --complete--> SUCCEEDED
       ^                  |
       |                  +--fail (attempts left)--> PENDING  (retried after backoff)
       |                  +--fail (no attempts left)--> FAILED
       +--lease expired---+

### Design decisions

| Problem | Solution |
|---|---|
| Two workers claim the same job | `SELECT ... FOR UPDATE SKIP LOCKED` (JPQL `@Lock(PESSIMISTIC_WRITE)` with lock-timeout hint `-2`). Locked rows are skipped, not waited on, so workers never block each other. |
| A worker crashes mid-job | **Leases.** Claiming records `locked_by` and `locked_until`. A reclaim step returns expired `RUNNING` jobs to `PENDING`. |
| A slow worker finishes after losing its lease | **Fencing.** `complete`/`fail` check that the caller still holds the lease and throw `LeaseLostException` if not. |
| Client retries a request | **Idempotency key.** Unique constraint on `idempotency_key`; submit looks up first, inserts, and on a race returns the winning row. |
| Retry storms | **Exponential backoff with full jitter** (base 2s, cap 5 min), so failed jobs do not all retry at the same instant. |
| Too much work for the workers | **Bounded pool + backpressure.** The poller only claims as many jobs as the pool has free capacity. |
| Lost updates on a row | `@Version` optimistic locking on top of the row locks. |
| Clean shutdown | The pool drains queued work and returns anything that never started. |

Delivery is **at-least-once**: if a lease expires while a handler is still running, the job can run again. Handlers should be idempotent.

### Main components

| Component | Responsibility |
|---|---|
| `Job` | JPA entity and the lifecycle state machine (`markRunning`, `markSucceeded`, `markFailed`) |
| `JobRepository` | Spring Data repository with the skip-locked claim query |
| `JobService` | Idempotent submit, complete, fail, find |
| `JobClaimService` | Claims batches under lock and reclaims expired leases |
| `JobPoller` | Scheduled loop that ties claiming, the pool and handlers together |
| `BoundedWorkerPool` | `ThreadPoolExecutor` with a bounded queue, rejection signal and graceful shutdown |
| `Backoff` | Exponential backoff with full jitter |
| `JobHandler` / `JobHandlerRegistry` | Plug-in point: one handler per job type |
| `JobController` | REST API |

---

## Run it

**Prerequisites:** Java 21, Docker.

    # 1. Start Postgres, Redis and Kafka
    docker compose up -d

    # 2. Point the app at your database (Spring Boot maps these env vars automatically)
    export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/jobforge
    export SPRING_DATASOURCE_USERNAME=<db user>
    export SPRING_DATASOURCE_PASSWORD=<db password>

    # 3. Run
    ./mvnw spring-boot:run

On Windows PowerShell use `$env:SPRING_DATASOURCE_URL="..."` and `.\mvnw spring-boot:run`. The schema is created automatically by Hibernate (`ddl-auto=update`). Redis must be running because the Redisson client connects at startup.

### Try it

    # Submit a job (the Idempotency-Key header is optional)
    curl -i -X POST http://localhost:8080/api/jobs \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: demo-1" \
      -d '{"type":"email","payload":"hello","priority":5}'

    # Check its status (use the id from the response)
    curl http://localhost:8080/api/jobs/<id>

Send the same request again with the same `Idempotency-Key` and you get the **same job id** back with no duplicate row.

### API

**`POST /api/jobs`** returns `202 Accepted` with a `Location` header and the job.

| Field | Required | Notes |
|---|---|---|
| `type` | yes | Selects the handler, max 100 chars, exact and case-sensitive match |
| `payload` | no | Free-form string, passed to the handler |
| `priority` | no | 0 to 100, higher runs first, default 0 |
| `runAt` | no | ISO-8601 instant, the job is not claimed before it. Default: now |
| `maxAttempts` | no | 1 to 20, default 5 |

Optional header `Idempotency-Key` (max 200 chars). Invalid input returns `400`.

**`GET /api/jobs/{id}`** returns the job, or `404`. A malformed id returns `400`.

**Built-in demo handlers:** `email` (succeeds) and `flaky` (fails twice, then succeeds, to demonstrate retries with backoff). Add your own by implementing `JobHandler` and registering it as a Spring `@Component`.

---

## Tests

    ./mvnw test       no Docker
    ./mvnw verify    # unit tests + integration tests (Docker required)

37 tests in total.

| Class | Type | What it proves |
|---|---|---|
| `BackoffTest` | unit | Delay doubles per attempt, never exceeds the cap, jitter stays in bounds and really spreads |
| `JobTest` | unit | Lifecycle transitions, lease checks, retry vs terminal failure |
| `BoundedWorkerPoolTest` | unit | Backpressure when full, graceful shutdown, tasks returned on timeout |
| `JobConcurrencyIT` | integration | 1000 threads fight over one job and exactly one wins. 40 workers drain 500 jobs with no duplicates and no losses. 200 concurrent submits with one key create one job. Priority order is respected. An expired lease is recovered and the stale worker is rejected |
| `JobControllerIT` | integration | HTTP contract: 202/400/404, idempotency header, validation |
| `JobEndToEndIT` | integration | Full pipeline with the poller running: success, retry with backoff, failure after max attempts |
| `...ApplicationTests` | integration | The application context starts |

Integration tests use **Testcontainers**, so they run against real PostgreSQL and Redis containers and never touch your local database. The concurrency tests use a `CyclicBarrier` so all threads start at the same instant, and Awaitility instead of fixed sleeps.

> **Docker Engine 29 note:** Testcontainers 1.21 sends an API version that Docker Engine 29 rejects. This repo includes `src/test/resources/docker-java.properties` with `api.version=1.44` to fix that.

---

## Known limitations and roadmap

- **No lease renewal yet.** A handler that runs longer than the lease (30s by default) can have its job reclaimed and run a second time. Planned: heartbeat that extends the lease.
- **Redis and Kafka are in the stack but not used by the core flow yet.** Planned: dead-letter queue and outbox via Kafka, Redis-based scheduling and leader election.
- Planned: Prometheus metrics (Micrometer is already on the classpath).
- Handler lookup is an exact, case-sensitive match on the job type.

## Configuration

Defined in `application.properties` under `jobforge.*`:

| Property | Default | Meaning |
|---|---|---|
| `jobforge.poller.enabled` | `true` | Turns the polling loop on or off |
| `jobforge.poller.interval` | `PT0.5S` | Delay between polls |
| `jobforge.worker.threads` | `8` | Worker threads |
| `jobforge.worker.queue-capacity` | `64` | Tasks waiting for a thread |
| `jobforge.worker.lease` | `30s` | How long a claimed job is reserved |
| `jobforge.worker.shutdown-timeout` | `30s` | Time allowed to drain on shutdown |
