package com.akkulov.reactive_learning.modules.V7_reactive_feign_resilience_practice.lesson07.http;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Оставляем только поля DummyJSON, которые нужны нашей лекции.
 * Остальные поля внешнего JSON Jackson безопасно проигнорирует.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Lesson07Product(
        long id,
        String title,
        BigDecimal price,
        String category,
        int stock
) {
}
