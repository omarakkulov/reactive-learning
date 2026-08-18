package com.akkulov.reactive_learning.modules.V7_reactive_feign_resilience_practice.lesson07.http;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactivefeign.client.ReactiveFeignException;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/api/lesson-07")
@RequiredArgsConstructor
public class Lesson07Controller {

    private static final Duration EXTERNAL_CALL_TIMEOUT = Duration.ofSeconds(3);

    private final Lesson07ProductReactiveClient productClient;

    @GetMapping("/products/{productId}")
    public Mono<ResponseEntity<Lesson07CallResult>> findProduct(
            @PathVariable long productId,
            @RequestParam(defaultValue = "0") long delayMs
    ) {
        log.info("Получен запрос продукта: productId={}, delayMs={}", productId, delayMs);

        if (productId < 1) {
            log.warn("Запрос отклонён: productId должен быть положительным, получено={}", productId);
            return Mono.just(failure(
                    HttpStatus.BAD_REQUEST,
                    Lesson07CallResult.FailureType.INVALID_REQUEST,
                    null,
                    "productId должен быть положительным"
            ));
        }
        if (delayMs < 0 || delayMs > 5_000) {
            log.warn("Запрос отклонён: delayMs должен быть в диапазоне 0..5000, получено={}", delayMs);
            return Mono.just(failure(
                    HttpStatus.BAD_REQUEST,
                    Lesson07CallResult.FailureType.INVALID_REQUEST,
                    null,
                    "delayMs должен быть в диапазоне 0..5000"
            ));
        }

        return handleExternalCall(productClient.findProduct(productId, delayMs));
    }

    @GetMapping("/unavailable")
    public Mono<ResponseEntity<Lesson07CallResult>> serviceUnavailable() {
        log.info("Запущен сценарий недоступного сервиса: одна попытка и два повтора");

        return handleExternalCall(
                // Этот endpoint всегда получает 503, поэтому для демонстрации
                // просто делаем ещё две попытки после первой ошибки.
                productClient.respondWithServiceUnavailable()
                        // retry повторно подписывается на upstream, поэтому эти два лога
                        // появятся для каждой из трёх попыток без RetrySignal и счётчиков.
                        .doOnSubscribe(ignored ->
                                log.info("Началась попытка вызова endpoint, возвращающего 503")
                        )
                        .doOnError(error ->
                                log.warn("Попытка завершилась ошибкой: type={}",
                                        error.getClass().getSimpleName())
                        )
                        .retry(2)
        );
    }

    private Mono<ResponseEntity<Lesson07CallResult>> handleExternalCall(
            Mono<Lesson07Product> externalCall
    ) {
        return externalCall
                // Сам HTTP-вызов начнётся только при подписке WebFlux на этот Mono.
                .doOnSubscribe(ignored ->
                        log.info("WebFlux подписался на цепочку: начинаем внешний HTTP-вызов")
                )
                // Не ждём внешний сервис бесконечно.
                .timeout(EXTERNAL_CALL_TIMEOUT)
                .doOnSuccess(it ->
                        log.info("Успешно получен ответ от внешнего сервиса: {}", it)
                )
                // Успешный onNext превращаем в обычный HTTP 200.
                .<ResponseEntity<Lesson07CallResult>>map(product ->
                        ResponseEntity.ok(new Lesson07CallResult.Success(product))
                )
                // timeout — первая ожидаемая error-ветка.
                .onErrorResume(TimeoutException.class, error -> {
                    log.warn("Внешний сервис не ответил за {} ms",
                            EXTERNAL_CALL_TIMEOUT.toMillis());

                    return Mono.just(failure(
                                HttpStatus.GATEWAY_TIMEOUT,
                                Lesson07CallResult.FailureType.TIMEOUT,
                                null,
                                "DummyJSON не ответил за 3000 ms"
                    ));
                })
                // FeignException означает, что внешний HTTP response получен.
                .onErrorResume(FeignException.class, error -> {
                    log.warn("Внешний сервис вернул HTTP status={}", error.status());

                    return Mono.just(failureFromHttpStatus(error));
                })
                // Здесь HTTP response нет: например, DNS или соединение завершились ошибкой.
                .onErrorResume(ReactiveFeignException.class, error -> {
                    log.error("HTTP response не получен из-за транспортной ошибки", error);

                    return Mono.just(failure(
                                HttpStatus.BAD_GATEWAY,
                                Lesson07CallResult.FailureType.TRANSPORT,
                                null,
                                "Не удалось выполнить HTTP-вызов DummyJSON"
                    ));
                });
    }

    private ResponseEntity<Lesson07CallResult> failureFromHttpStatus(
            FeignException error
    ) {
        if (error.status() == 404) {
            return failure(
                    HttpStatus.NOT_FOUND,
                    Lesson07CallResult.FailureType.NOT_FOUND,
                    404,
                    "DummyJSON не нашёл продукт"
            );
        }

        return failure(
                HttpStatus.SERVICE_UNAVAILABLE,
                Lesson07CallResult.FailureType.UPSTREAM_UNAVAILABLE,
                error.status(),
                "DummyJSON вернул HTTP " + error.status()
        );
    }

    private ResponseEntity<Lesson07CallResult> failure(
            HttpStatus responseStatus,
            Lesson07CallResult.FailureType failureType,
            Integer externalStatus,
            String message
    ) {
        return ResponseEntity.status(responseStatus)
                .body(
                        new Lesson07CallResult.Failure(
                                failureType,
                                externalStatus,
                                message
                        )
                );
    }
}
