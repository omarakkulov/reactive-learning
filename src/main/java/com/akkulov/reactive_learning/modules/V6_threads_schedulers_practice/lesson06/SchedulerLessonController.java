package com.akkulov.reactive_learning.modules.V6_threads_schedulers_practice.lesson06;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
@RestController
@RequestMapping("/api/lesson-06")
public class SchedulerLessonController {

    private static final long MIN_DURATION_MS = 1;
    private static final long MAX_DURATION_MS = 5_000;

    private final CpuIntensiveCryptoService cryptoService;
    private final LegacyBlockingClient blockingClient;

    public SchedulerLessonController(
            CpuIntensiveCryptoService cryptoService,
            LegacyBlockingClient blockingClient
    ) {
        this.cryptoService = cryptoService;
        this.blockingClient = blockingClient;
    }

    @GetMapping("/current-thread")
    public Mono<Lesson06ExecutionResponse> currentThread() {
        String controllerThread = currentThreadName();
        logController("CURRENT-THREAD", controllerThread, 0);

        return Mono.fromSupplier(() -> {
            String workThread = currentThreadName();
            log.info("[CURRENT-THREAD] source выполняется | thread={}", workThread);
            return new Lesson06ExecutionResponse(
                    "current-thread",
                    controllerThread,
                    workThread,
                    0,
                    0,
                    "no-scheduler-boundary"
            );
        });
    }

    /**
     * Намеренно неправильный учебный endpoint: CPU-bound работа выполняется на event loop.
     */
    @GetMapping("/cpu-on-event-loop")
    public Mono<Lesson06ExecutionResponse> cpuOnEventLoop(
            @RequestParam(defaultValue = "reactive") String payload,
            @RequestParam(defaultValue = "3000") long durationMs
    ) {
        long checkedDurationMs = validateDuration(durationMs);
        String controllerThread = currentThreadName();
        logController("CPU-WRONG", controllerThread, checkedDurationMs);

        return Mono.just(payload)
                .map(value ->
                        executeCpuWork(
                                "cpu-on-event-loop",
                                controllerThread,
                                value,
                                checkedDurationMs
                        ));
    }

    /**
     * CPU-bound стадия после publishOn выполняется на общем Reactor parallel Scheduler.
     */
    @GetMapping("/cpu-on-parallel")
    public Mono<Lesson06ExecutionResponse> cpuOnParallel(
            @RequestParam(defaultValue = "reactive") String payload,
            @RequestParam(defaultValue = "3000") long durationMs
    ) {
        long checkedDurationMs = validateDuration(durationMs);
        String controllerThread = currentThreadName();
        logController("CPU-CORRECT", controllerThread, checkedDurationMs);

        return Mono.just(payload)
                .doOnNext(value -> log.info(
                        "[CPU-CORRECT] выполнение doOnNext на потоке thread={}",
                        currentThreadName()
                ))
                .publishOn(Schedulers.parallel())
                .map(value -> executeCpuWork(
                        "cpu-on-parallel",
                        controllerThread,
                        value,
                        checkedDurationMs
                ));
    }

    /**
     * Намеренно неправильный учебный endpoint: lazy blocking source не получил Scheduler boundary.
     */
    @GetMapping("/blocking-on-event-loop")
    public Mono<Lesson06ExecutionResponse> blockingOnEventLoop(
            @RequestParam(defaultValue = "42") String userId,
            @RequestParam(defaultValue = "3000") long durationMs
    ) {
        long checkedDurationMs = validateDuration(durationMs);
        String controllerThread = currentThreadName();
        logController("BLOCKING-WRONG", controllerThread, checkedDurationMs);

        return Mono.fromCallable(() -> executeBlockingWork(
                "blocking-on-event-loop",
                controllerThread,
                userId,
                checkedDurationMs
        ));
    }

    /**
     * Правильный blocking adapter: source остаётся lazy, а subscribeOn переносит его выполнение.
     */
    @GetMapping("/blocking-on-bounded-elastic")
    public Mono<Lesson06ExecutionResponse> blockingOnBoundedElastic(
            @RequestParam(defaultValue = "42") String userId,
            @RequestParam(defaultValue = "3000") long durationMs
    ) {
        long checkedDurationMs = validateDuration(durationMs);
        String controllerThread = currentThreadName();
        logController("BLOCKING-CORRECT", controllerThread, checkedDurationMs);

        return Mono.fromCallable(() -> executeBlockingWork(
                        "blocking-on-bounded-elastic",
                        controllerThread,
                        userId,
                        checkedDurationMs
                ))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Lesson06ExecutionResponse executeCpuWork(
            String scenario,
            String controllerThread,
            String payload,
            long durationMs
    ) {
        String workThread = currentThreadName();
        long startedNanos = System.nanoTime();
        log.info("[{}] CPU work START | thread={}", scenario, workThread);

        CpuIntensiveCryptoService.CryptoComputation computation =
                cryptoService.repeatedlyHashFor(payload, durationMs);

        long actualDurationMs = elapsedMillis(startedNanos);
        log.info(
                "[{}] CPU work END, iterations={}, actualDurationMs={} | thread={}",
                scenario,
                computation.iterations(),
                actualDurationMs,
                currentThreadName()
        );

        return new Lesson06ExecutionResponse(
                scenario,
                controllerThread,
                workThread,
                durationMs,
                actualDurationMs,
                "sha256=" + computation.hash() + ", iterations=" + computation.iterations()
        );
    }

    private Lesson06ExecutionResponse executeBlockingWork(
            String scenario,
            String controllerThread,
            String userId,
            long durationMs
    ) {
        String workThread = currentThreadName();
        long startedNanos = System.nanoTime();
        log.info("[{}] blocking call START | thread={}", scenario, workThread);

        String result = blockingClient.loadProfile(userId, durationMs);

        long actualDurationMs = elapsedMillis(startedNanos);
        log.info(
                "[{}] blocking call END, actualDurationMs={} | thread={}",
                scenario,
                actualDurationMs,
                currentThreadName()
        );

        return new Lesson06ExecutionResponse(
                scenario,
                controllerThread,
                workThread,
                durationMs,
                actualDurationMs,
                result
        );
    }

    private long validateDuration(long durationMs) {
        if (durationMs < MIN_DURATION_MS || durationMs > MAX_DURATION_MS) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "durationMs должен быть в диапазоне " + MIN_DURATION_MS + ".." + MAX_DURATION_MS
            );
        }
        return durationMs;
    }

    private void logController(String scenario, String controllerThread, long durationMs) {
        log.info(
                "[{}] controller method, durationMs={} | thread={}",
                scenario,
                durationMs,
                controllerThread
        );
    }

    private long elapsedMillis(long startedNanos) {
        return Math.max(1, (System.nanoTime() - startedNanos) / 1_000_000);
    }

    private String currentThreadName() {
        return Thread.currentThread().getName();
    }
}
