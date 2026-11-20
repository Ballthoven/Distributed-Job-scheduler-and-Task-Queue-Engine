package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.web;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker.BoundedWorkerPool;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
class BoundedWorkerPoolTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void rejectsWorkOnceThreadsAndQueueAreFull() throws Exception {
        try (BoundedWorkerPool pool = new BoundedWorkerPool("test", 2, 3, TIMEOUT)) {
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger completed = new AtomicInteger();
            Runnable blocked = () -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                completed.incrementAndGet();
            };

             for (int i = 0; i < 5; i++) {
                assertThat(pool.trySubmit(blocked)).as("task %d accepted", i).isTrue();
            }
            // ...and the 6th is refused: that is the backpressure signal.
            assertThat(pool.trySubmit(blocked)).isFalse();
            assertThat(pool.remainingCapacity()).isZero();

            release.countDown();
            assertThat(pool.shutdownGracefully(TIMEOUT)).isEmpty();
            assertThat(completed).hasValue(5);
        }
    }

    @Test
    void neverRunsMoreTasksAtOnceThanItHasThreads() throws Exception {
        int threads = 4;
        int tasks = 300;
        try (BoundedWorkerPool pool = new BoundedWorkerPool("test", threads, 8, TIMEOUT)) {
            AtomicInteger running = new AtomicInteger();
            AtomicInteger maxRunning = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(tasks);

            Runnable task = () -> {
                int now = running.incrementAndGet();
                maxRunning.accumulateAndGet(now, Math::max);
                Thread.onSpinWait();
                running.decrementAndGet();
                done.countDown();
            };

            for (int i = 0; i < tasks; i++) {
                while (!pool.trySubmit(task)) {
                    Thread.yield();
                }
            }

            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(maxRunning.get()).isBetween(1, threads);
        }
    }

    @Test
    void gracefulShutdownLetsQueuedWorkFinish() throws Exception {
        BoundedWorkerPool pool = new BoundedWorkerPool("test", 1, 10, TIMEOUT);
        AtomicInteger completed = new AtomicInteger();
        for (int i = 0; i < 6; i++) {
            assertThat(pool.trySubmit(() -> {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                completed.incrementAndGet();
            })).isTrue();
        }

        List<Runnable> neverRan = pool.shutdownGracefully(TIMEOUT);

        assertThat(neverRan).isEmpty();
        assertThat(completed).hasValue(6);
    }

    @Test
    void shutdownGivesBackTasksThatNeverStartedWhenTheDeadlinePasses() throws Exception {
        BoundedWorkerPool pool = new BoundedWorkerPool("test", 1, 10, TIMEOUT);
        CountDownLatch started = new CountDownLatch(1);
        assertThat(pool.trySubmit(() -> {
            started.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        })).isTrue();
        assertThat(pool.trySubmit(() -> {
        })).isTrue();
        assertThat(pool.trySubmit(() -> {
        })).isTrue();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        List<Runnable> neverRan = pool.shutdownGracefully(Duration.ofMillis(100));

        assertThat(neverRan).hasSize(2);
    }

    @Test
    void refusesToAcceptWorkAfterShutdown() {
        BoundedWorkerPool pool = new BoundedWorkerPool("test", 1, 1, TIMEOUT);
        pool.shutdownGracefully(TIMEOUT);

        assertThat(pool.trySubmit(() -> {
        })).isFalse();
    }

    @Test
    void rejectsInvalidSizing() {
        assertThatThrownBy(() -> new BoundedWorkerPool("test", 0, 1, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedWorkerPool("test", 1, 0, TIMEOUT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}