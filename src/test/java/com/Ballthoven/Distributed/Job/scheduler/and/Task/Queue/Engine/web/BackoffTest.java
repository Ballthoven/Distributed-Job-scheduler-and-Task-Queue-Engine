package com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.web;

import com.Ballthoven.Distributed.Job.scheduler.and.Task.Queue.Engine.worker.BackOff;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;


class BackoffTest {

    private static final Duration BASE = Duration.ofSeconds(2);
    private static final Duration CAP = Duration.ofMinutes(5);

    @Test
    void ceilingDoublesEachAttemptUntilTheCap() {
        assertThat(BackOff.ceiling(1, BASE, CAP)).isEqualTo(Duration.ofSeconds(2));
        assertThat(BackOff.ceiling(2, BASE, CAP)).isEqualTo(Duration.ofSeconds(4));
        assertThat(BackOff.ceiling(3, BASE, CAP)).isEqualTo(Duration.ofSeconds(8));
        assertThat(BackOff.ceiling(4, BASE, CAP)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    void ceilingNeverExceedsTheCapEvenForAbsurdAttemptCounts() {
        assertThat(BackOff.ceiling(50, BASE, CAP)).isEqualTo(CAP);
        assertThat(BackOff.ceiling(Integer.MAX_VALUE, BASE, CAP)).isEqualTo(CAP);
    }

    @Test
    void jitteredDelayStaysWithinZeroAndTheCeiling() {
        Random random = new Random(42);
        for (int attempt = 1; attempt <= 30; attempt++) {
            Duration ceiling = BackOff.ceiling(attempt, BASE, CAP);
            for (int i = 0; i < 200; i++) {
                Duration delay = BackOff.fullJitter(attempt, BASE, CAP, random);
                assertThat(delay).isBetween(Duration.ZERO, ceiling);
            }
        }
    }

    @Test
    void jitterActuallySpreadsTheDelays() {
        Random random = new Random(7);
        long distinct = java.util.stream.IntStream.range(0, 200)
                .mapToObj(i -> BackOff.fullJitter(5, BASE, CAP, random))
                .distinct()
                .count();

        assertThat(distinct).isGreaterThan(50);
    }
}

