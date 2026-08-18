package com.akkulov.reactive_learning.modules.V7_reactive_feign_resilience_practice.lesson07.http;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import reactivefeign.spring.config.ReactiveFeignClient;
import reactor.core.publisher.Mono;

@ReactiveFeignClient(
        name = "lesson07-product-service",
        url = "${lesson07.product-service.url:https://dummyjson.com}"
)
public interface Lesson07ProductReactiveClient {

    @GetMapping("/products/{productId}")
    Mono<Lesson07Product> findProduct(
            @PathVariable("productId") long productId,
            @RequestParam("delay") long delayMs
    );

    // DummyJSON намеренно вернёт 503. Успешный body здесь не нужен:
    // метод существует только для наглядной демонстрации retry.
    @GetMapping("/http/503/lesson-07")
    Mono<Lesson07Product> respondWithServiceUnavailable();
}
