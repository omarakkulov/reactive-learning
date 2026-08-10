package com.akkulov.reactive_learning.modules.V6_threads_schedulers_practice.lesson06;

import org.springframework.stereotype.Component;

/**
 * Имитирует синхронный JDBC/legacy HTTP/file вызов, во время которого Thread ждёт.
 */
@Component
public class LegacyBlockingClient {

    public String loadProfile(String userId, long durationMs) {
        try {
            Thread.sleep(durationMs);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Учебный blocking-вызов был прерван", error);
        }

        return "legacy-profile-for-" + userId;
    }
}
