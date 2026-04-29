package com.akkulov.reactive_learning.modules.V1_blocking_vs_nonblocking.lesson01;

import java.time.Duration;
import java.time.Instant;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/api/lesson-01")
public class BlockingVsNonBlockingController {

    @GetMapping("/thread")
    Mono<ThreadReport> currentThread() {
        String threadName = Thread.currentThread().getName();
        log.info("Thread report requested on thread={}", threadName);

        return Mono.just(new ThreadReport(threadName));
    }

    @GetMapping("/blocking-sleep")
    Mono<DelayReport> blockingSleep(@RequestParam(defaultValue = "1000") long delayMs) throws InterruptedException {
        Instant startedAt = Instant.now();
        String startedOn = Thread.currentThread().getName();
        log.info("Blocking sleep started: delayMs={}, thread={}", delayMs, startedOn);

        Thread.sleep(delayMs);

        String completedOn = Thread.currentThread().getName();
        long elapsedMs = Duration.between(startedAt, Instant.now()).toMillis();
        log.info("Blocking sleep completed: elapsedMs={}, thread={}", elapsedMs, completedOn);

        return Mono.just(new DelayReport("blocking", delayMs, elapsedMs, startedOn, completedOn));
    }

    @GetMapping("/non-blocking-delay")
    Mono<DelayReport> nonBlockingDelay(@RequestParam(defaultValue = "1000") long delayMs) {
        Instant startedAt = Instant.now();
        String startedOn = Thread.currentThread().getName();
        log.info("Non-blocking delay scheduled: delayMs={}, thread={}", delayMs, startedOn);

        return Mono.delay(Duration.ofMillis(delayMs))
                .map(ignored -> {
                    String completedOn = Thread.currentThread().getName();
                    long elapsedMs = Duration.between(startedAt, Instant.now()).toMillis();
                    log.info("Non-blocking delay completed: elapsedMs={}, thread={}", elapsedMs, completedOn);
                    return new DelayReport("non-blocking", delayMs, elapsedMs, startedOn, completedOn);
                });
    }
}
