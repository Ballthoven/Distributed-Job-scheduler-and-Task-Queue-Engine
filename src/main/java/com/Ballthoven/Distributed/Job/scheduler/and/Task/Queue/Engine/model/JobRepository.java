package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.model;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import org.springframework.data.domain.Pageable;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    String LOCK_TIMEOUT_HINT = "jakarta.persistence.lock.timeout";
    String SKIP_LOCKED = "-2";

    Optional<Job> findByIdempotencyKey(String idempotencyKey);

       @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = LOCK_TIMEOUT_HINT, value = SKIP_LOCKED))
    @Query("""
            select j from Job j
            where j.status = :status and j.runAt <= :now
            order by j.priority desc, j.runAt asc
            """)
    List<Job> lockNextBatch(@Param("status") JobStatus status,
                            @Param("now") Instant now,
                            Pageable pageable);
  @Modifying(clearAutomatically = true)
    @Query(value = """
            UPDATE jobs
            SET status = 'PENDING', locked_by = NULL, locked_until = NULL,
                version = version + 1, updated_at = :now
            WHERE status = 'RUNNING' AND locked_until < :now
            """, nativeQuery = true)
    int reclaimExpiredLeases(@Param("now") Instant now);
}
