package com.akkulov.reactive_learning.modules.V7_reactive_feign_resilience_practice.lesson07.http;

/**
 * Явный результат внешнего вызова: downstream получает значение одной из двух веток,
 * а не должен угадывать, какое исключение может прилететь из HTTP-клиента.
 */
public sealed interface Lesson07CallResult
        permits Lesson07CallResult.Success, Lesson07CallResult.Failure {

    record Success(Lesson07Product value) implements Lesson07CallResult {
    }

    record Failure(
            FailureType type,
            Integer externalStatus,
            String message
    ) implements Lesson07CallResult {
    }

    enum FailureType {
        INVALID_REQUEST,
        NOT_FOUND,
        TIMEOUT,
        UPSTREAM_UNAVAILABLE,
        TRANSPORT,
        UNEXPECTED
    }
}
