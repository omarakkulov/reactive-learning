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

    /**
     * Составной правильный сценарий:
     * blocking source выполняется на boundedElastic, а последующая CPU-bound стадия — на parallel.
     */
    @GetMapping("/blocking-then-cpu")
    public Mono<Lesson06CombinedExecutionResponse> blockingThenCpu(
            @RequestParam(defaultValue = "42") String userId,
            @RequestParam(defaultValue = "1500") long blockingDurationMs,
            @RequestParam(defaultValue = "3000") long cpuDurationMs
    ) {
        long checkedBlockingDurationMs = validateDuration(blockingDurationMs);
        long checkedCpuDurationMs = validateDuration(cpuDurationMs);
        String controllerThread = currentThreadName();

        log.info(
                "[COMBINED] controller method, blockingDurationMs={}, cpuDurationMs={} | thread={}",
                checkedBlockingDurationMs,
                checkedCpuDurationMs,
                controllerThread
        );

        return Mono.fromCallable(() -> executeBlockingStage(
                        userId,
                        checkedBlockingDurationMs
                ))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(stage -> log.info(
                        "[COMBINED] blocking result перед publishOn | thread={}",
                        currentThreadName()
                ))
                .publishOn(Schedulers.parallel())
                .map(stage -> executeCombinedCpuStage(
                        controllerThread,
                        stage,
                        checkedBlockingDurationMs,
                        checkedCpuDurationMs
                ));
    }

    /**
     * Зеркальный составной сценарий:
     * CPU-bound стадия выполняется на parallel, затем blocking source — на boundedElastic.
     */
    @GetMapping("/cpu-then-blocking")
    public Mono<Lesson06CombinedExecutionResponse> cpuThenBlocking(
            @RequestParam(defaultValue = "reactive") String payload,
            @RequestParam(defaultValue = "42") String userId,
            @RequestParam(defaultValue = "3000") long cpuDurationMs,
            @RequestParam(defaultValue = "1500") long blockingDurationMs
    ) {
        long checkedCpuDurationMs = validateDuration(cpuDurationMs);
        long checkedBlockingDurationMs = validateDuration(blockingDurationMs);
        String controllerThread = currentThreadName();

        log.info(
                "[CPU-THEN-BLOCKING] controller method, cpuDurationMs={}, blockingDurationMs={} | thread={}",
                checkedCpuDurationMs,
                checkedBlockingDurationMs,
                controllerThread
        );

        return Mono.just(payload)
                .publishOn(Schedulers.parallel())
                .map(value -> executeCombinedCpuStage(
                        value,
                        checkedCpuDurationMs
                ))
                .flatMap(
                        cpuStage -> Mono.fromCallable(() -> executeBlockingAfterCpuStage(
                                        controllerThread,
                                        cpuStage,
                                        userId,
                                        checkedCpuDurationMs,
                                        checkedBlockingDurationMs
                                ))
                                .subscribeOn(Schedulers.boundedElastic())
                );
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

    private BlockingStageResult executeBlockingStage(String userId, long durationMs) {
        String blockingThread = currentThreadName();
        long startedNanos = System.nanoTime();
        log.info("[COMBINED] blocking stage START | thread={}", blockingThread);

        String profile = blockingClient.loadProfile(userId, durationMs);

        long actualDurationMs = elapsedMillis(startedNanos);
        log.info(
                "[COMBINED] blocking stage END, actualDurationMs={} | thread={}",
                actualDurationMs,
                currentThreadName()
        );
        return new BlockingStageResult(profile, blockingThread, actualDurationMs);
    }

    private Lesson06CombinedExecutionResponse executeCombinedCpuStage(
            String controllerThread,
            BlockingStageResult blockingStage,
            long requestedBlockingDurationMs,
            long requestedCpuDurationMs
    ) {
        String cpuThread = currentThreadName();
        long startedNanos = System.nanoTime();
        log.info("[COMBINED] CPU stage START | thread={}", cpuThread);

        CpuIntensiveCryptoService.CryptoComputation computation =
                cryptoService.repeatedlyHashFor(blockingStage.profile(), requestedCpuDurationMs);

        long actualCpuDurationMs = elapsedMillis(startedNanos);
        log.info(
                "[COMBINED] CPU stage END, iterations={}, actualDurationMs={} | thread={}",
                computation.iterations(),
                actualCpuDurationMs,
                currentThreadName()
        );

        return new Lesson06CombinedExecutionResponse(
                "blocking-then-cpu",
                controllerThread,
                blockingStage.thread(),
                cpuThread,
                requestedBlockingDurationMs,
                blockingStage.actualDurationMs(),
                requestedCpuDurationMs,
                actualCpuDurationMs,
                "profile=" + blockingStage.profile()
                        + ", sha256=" + computation.hash()
                        + ", iterations=" + computation.iterations()
        );
    }

    private CpuStageResult executeCombinedCpuStage(String payload, long durationMs) {
        String cpuThread = currentThreadName();
        long startedNanos = System.nanoTime();
        log.info("[CPU-THEN-BLOCKING] CPU stage START | thread={}", cpuThread);

        CpuIntensiveCryptoService.CryptoComputation computation =
                cryptoService.repeatedlyHashFor(payload, durationMs);

        long actualDurationMs = elapsedMillis(startedNanos);
        log.info(
                "[CPU-THEN-BLOCKING] CPU stage END, iterations={}, actualDurationMs={} | thread={}",
                computation.iterations(),
                actualDurationMs,
                currentThreadName()
        );
        return new CpuStageResult(computation, cpuThread, actualDurationMs);
    }

    private Lesson06CombinedExecutionResponse executeBlockingAfterCpuStage(
            String controllerThread,
            CpuStageResult cpuStage,
            String userId,
            long requestedCpuDurationMs,
            long requestedBlockingDurationMs
    ) {
        String blockingThread = currentThreadName();
        long startedNanos = System.nanoTime();
        log.info("[CPU-THEN-BLOCKING] blocking stage START | thread={}", blockingThread);

        String profile = blockingClient.loadProfile(userId, requestedBlockingDurationMs);

        long actualBlockingDurationMs = elapsedMillis(startedNanos);
        log.info(
                "[CPU-THEN-BLOCKING] blocking stage END, actualDurationMs={} | thread={}",
                actualBlockingDurationMs,
                currentThreadName()
        );

        return new Lesson06CombinedExecutionResponse(
                "cpu-then-blocking",
                controllerThread,
                blockingThread,
                cpuStage.thread(),
                requestedBlockingDurationMs,
                actualBlockingDurationMs,
                requestedCpuDurationMs,
                cpuStage.actualDurationMs(),
                "sha256=" + cpuStage.computation().hash()
                        + ", iterations=" + cpuStage.computation().iterations()
                        + ", profile=" + profile
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

    private record BlockingStageResult(
            String profile,
            String thread,
            long actualDurationMs
    ) {
    }

    private record CpuStageResult(
            CpuIntensiveCryptoService.CryptoComputation computation,
            String thread,
            long actualDurationMs
    ) {
    }
}
