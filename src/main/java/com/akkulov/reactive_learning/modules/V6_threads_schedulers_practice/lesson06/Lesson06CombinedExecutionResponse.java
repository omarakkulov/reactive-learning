package com.akkulov.reactive_learning.modules.V6_threads_schedulers_practice.lesson06;

/**
 * Ответ составного сценария: blocking source на boundedElastic, затем CPU stage на parallel.
 */
public record Lesson06CombinedExecutionResponse(
        String scenario,
        String controllerThread,
        String blockingThread,
        String cpuThread,
        long requestedBlockingDurationMs,
        long actualBlockingDurationMs,
        long requestedCpuDurationMs,
        long actualCpuDurationMs,
        String result
) {
}
