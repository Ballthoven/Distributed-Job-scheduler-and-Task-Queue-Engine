package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public final class BoundedWorkerPool implements AutoCloseable {

    private final ThreadPoolExecutor executor;
    private final Duration shutdownTimeout;

    public BoundedWorkerPool(String name, int threads, int queueCapacity, Duration shutdownTimeout) {
        if (threads < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("threads and queueCapacity must be >= 1");
        }
        this.shutdownTimeout = shutdownTimeout;
        this.executor = new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                namedThreads(name),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public boolean trySubmit(Runnable task) {
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException rejected) {
            return false;
        }
    }

     public int remainingCapacity() {
        int idleThreads = executor.getCorePoolSize() - executor.getActiveCount();
        return executor.getQueue().remainingCapacity() + Math.max(0, idleThreads);
    }

    public int activeThreads() {
        return executor.getActiveCount();
    }

    public int queueDepth() {
        return executor.getQueue().size();
    }

    public long completedTasks() {
        return executor.getCompletedTaskCount();
    }

    public List<Runnable> shutdownGracefully(Duration timeout) {
        executor.shutdown();
        try {
            if (executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.info("Worker pool drained cleanly");
                return List.of();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        List<Runnable> neverRan = executor.shutdownNow();
        log.warn("Worker pool did not drain within {}; interrupted running tasks and dropped {} queued task(s)",
                timeout, neverRan.size());
        return neverRan;
    }

    @Override
    public void close() {
        shutdownGracefully(shutdownTimeout);
    }

    private static ThreadFactory namedThreads(String name) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, name + "-worker-" + counter.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
    }
}
