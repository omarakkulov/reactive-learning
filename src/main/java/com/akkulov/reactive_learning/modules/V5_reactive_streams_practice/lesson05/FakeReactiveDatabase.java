package com.akkulov.reactive_learning.modules.V5_reactive_streams_practice.lesson05;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Mono;

/**
 * Учебная база данных. Она ничего не читает из сети или с диска: значения
 * захардкожены, чтобы лекция была посвящена Reactive Streams Protocol, а не I/O.
 */
public final class FakeReactiveDatabase {

    public static final long EXISTING_USER_ID = 42L;
    public static final long MISSING_USER_ID = 404L;
    public static final long FAILING_USER_ID = -1L;

    private final Map<Long, UserRecord> users = Map.of(
            EXISTING_USER_ID,
            new UserRecord(EXISTING_USER_ID, "Omar", true)
    );
    private final AtomicInteger lookupCount = new AtomicInteger();

    /**
     * Создаёт описание запроса, но не выполняет lookup.
     */
    public Publisher<UserRecord> findUserById(long userId) {
        System.out.println("[ASSEMBLY] database.findUserById(" + userId
                + ") создаёт Publisher; lookup ещё не выполнен");
        return new DatabaseQueryPublisher(userId, this);
    }

    /**
     * Второй асинхронный по форме шаг для демонстрации flatMap.
     */
    public Mono<String> loadGreeting(String name) {
        System.out.println("[ASSEMBLY] database.loadGreeting(" + name + ") создаёт Mono");
        return Mono.fromSupplier(() -> {
            System.out.println("[QUERY] создаём персональное приветствие для " + name);
            return "Привет, " + name + "!";
        });
    }

    /**
     * Ленивый fallback для switchIfEmpty + Mono.defer.
     */
    public Mono<String> anonymousGreeting() {
        System.out.println("[ASSEMBLY] создаётся fallback Publisher");
        return Mono.fromSupplier(() -> {
            System.out.println("[QUERY] создаём анонимное приветствие");
            return "Привет, незнакомец!";
        });
    }

    public int lookupCount() {
        return lookupCount.get();
    }

    Optional<UserRecord> executeLookup(long userId) {
        int currentLookup = lookupCount.incrementAndGet();
        System.out.println("[QUERY] выполняется lookup #" + currentLookup + " для userId=" + userId);

        if (userId == FAILING_USER_ID) {
            throw new IllegalStateException("Гипотетическая БД недоступна");
        }

        return Optional.ofNullable(users.get(userId));
    }
}
