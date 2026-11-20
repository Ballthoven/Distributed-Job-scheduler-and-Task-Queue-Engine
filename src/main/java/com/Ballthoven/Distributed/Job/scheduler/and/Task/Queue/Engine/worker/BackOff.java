package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.random.RandomGenerator;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BackOff {
    public static Duration fullJitter(int attempt, Duration base, Duration cap, RandomGenerator random) {
        int exponent = Math.min(Math.max(attempt - 1, 0), 20);
        long ceilingMillis = Math.min(cap.toMillis(), base.toMillis() << exponent);
        return Duration.ofMillis(random.nextLong(ceilingMillis + 1));
    }
    public static Duration ceiling(int attempt, Duration base, Duration cap) {
        int exponent = Math.min(Math.max(attempt - 1, 0), 20);
        return Duration.ofMillis(Math.min(cap.toMillis(), base.toMillis() << exponent));
    }
}
