package com.akkulov.reactive_learning.modules.V6_threads_schedulers_practice.lesson06;

/**
 * Ответ учебных endpoint-ов шестой лекции.
 *
 * @param scenario сценарий, который был выполнен
 * @param controllerThread поток, в котором Spring вызвал controller method
 * @param workThread поток, в котором реально выполнялась долгая работа
 * @param requestedDurationMs запрошенная учебная длительность работы
 * @param actualDurationMs фактическая длительность работы
 * @param result короткий результат вычисления или blocking-вызова
 */
public record Lesson06ExecutionResponse(
        String scenario,
        String controllerThread,
        String workThread,
        long requestedDurationMs,
        long actualDurationMs,
        String result
) {
}
