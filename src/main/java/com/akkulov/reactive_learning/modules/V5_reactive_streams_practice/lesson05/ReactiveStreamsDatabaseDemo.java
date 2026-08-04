package com.akkulov.reactive_learning.modules.V5_reactive_streams_practice.lesson05;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Запускаемый из IDE чистый Java-сценарий пятой лекции.
 */
public final class ReactiveStreamsDatabaseDemo {

    private ReactiveStreamsDatabaseDemo() {
    }

    public static void main(String[] args) {
//        happyPath();
        emptyAndErrorPaths();
//        cancellationAndInvalidDemand();
//        coldPublisher();
//        reactorOperators();
//        fluxBackpressure();
    }

    private static void happyPath() {
        section("1. SUBSCRIBE ЕЩЁ НЕ ОЗНАЧАЕТ ПОЛУЧЕНИЕ ДАННЫХ");

        FakeReactiveDatabase database = new FakeReactiveDatabase();
        Publisher<UserRecord> query = database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID);
        ManualDemandSubscriber<UserRecord> user = new ManualDemandSubscriber<>("user-subscriber");

        query.subscribe(user);
        System.out.println("[CHECK] lookupCount после subscribe = " + database.lookupCount());

        user.request(1);
        System.out.println("[CHECK] lookupCount после request(1) = " + database.lookupCount());

        user.request(1);
        System.out.println("[CHECK] повторный request не изменил lookupCount = " + database.lookupCount());
    }

    private static void emptyAndErrorPaths() {
        section("2. MONO-ПОДОБНЫЕ ИСХОДЫ: EMPTY И ERROR");

        FakeReactiveDatabase database = new FakeReactiveDatabase();

        ManualDemandSubscriber<UserRecord> missing = new ManualDemandSubscriber<>("missing-user");
        database.findUserById(FakeReactiveDatabase.MISSING_USER_ID).subscribe(missing);
        missing.request(1);

        ManualDemandSubscriber<UserRecord> failed = new ManualDemandSubscriber<>("failed-query");
        database.findUserById(FakeReactiveDatabase.FAILING_USER_ID).subscribe(failed);
        failed.request(1);
    }

    private static void cancellationAndInvalidDemand() {
        section("3. CANCEL И НЕДОПУСТИМЫЙ DEMAND");

        FakeReactiveDatabase cancelledDatabase = new FakeReactiveDatabase();
        ManualDemandSubscriber<UserRecord> cancelled = new ManualDemandSubscriber<>("cancelled-user");
        cancelledDatabase.findUserById(FakeReactiveDatabase.EXISTING_USER_ID).subscribe(cancelled);
        cancelled.cancel();
        cancelled.cancel();
        cancelled.request(1);
        System.out.println("[CHECK] lookupCount после cancel = " + cancelledDatabase.lookupCount());

        FakeReactiveDatabase invalidDemandDatabase = new FakeReactiveDatabase();
        ManualDemandSubscriber<UserRecord> invalid = new ManualDemandSubscriber<>("invalid-demand-user");
        invalidDemandDatabase.findUserById(FakeReactiveDatabase.EXISTING_USER_ID).subscribe(invalid);
        invalid.request(0);
        invalid.request(1);
        System.out.println("[CHECK] lookupCount после request(0) = " + invalidDemandDatabase.lookupCount());
    }

    private static void coldPublisher() {
        section("4. COLD: ОДИН PUBLISHER, ДВЕ НЕЗАВИСИМЫЕ ПОДПИСКИ");

        FakeReactiveDatabase database = new FakeReactiveDatabase();
        Publisher<UserRecord> coldQuery = database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID);

        ManualDemandSubscriber<UserRecord> first = new ManualDemandSubscriber<>("first-subscriber");
        ManualDemandSubscriber<UserRecord> second = new ManualDemandSubscriber<>("second-subscriber");

        coldQuery.subscribe(first);
        coldQuery.subscribe(second);
        first.request(1);
        second.request(1);

        System.out.println("[CHECK] две cold-подписки выполнили lookupCount = " + database.lookupCount());
    }

    private static void reactorOperators() {
        section("5. REACTOR ОБОРАЧИВАЕТ КАСТОМНЫЙ PUBLISHER ОПЕРАТОРАМИ");

        FakeReactiveDatabase database = new FakeReactiveDatabase();
        Mono<String> greetingPipeline = Mono.from(database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID))
                .filter(UserRecord::active)
                .map(UserRecord::name)
                .flatMap(database::loadGreeting)
                .switchIfEmpty(Mono.defer(database::anonymousGreeting))
                .doOnNext(value -> System.out.println("[OPERATOR] doOnNext наблюдает: " + value));

        ManualDemandSubscriber<String> greetingSubscriber = new ManualDemandSubscriber<>("greeting-subscriber");
        greetingPipeline.subscribe(greetingSubscriber);
        greetingSubscriber.request(1);

        Mono<String> fallbackPipeline = Mono.from(database.findUserById(FakeReactiveDatabase.MISSING_USER_ID))
                .map(UserRecord::name)
                .flatMap(database::loadGreeting)
                .switchIfEmpty(Mono.defer(database::anonymousGreeting));

        ManualDemandSubscriber<String> fallbackSubscriber = new ManualDemandSubscriber<>("fallback-subscriber");
        fallbackPipeline.subscribe(fallbackSubscriber);
        fallbackSubscriber.request(1);
    }

    private static void fluxBackpressure() {
        section("6. FLUX: REQUEST(1) ДЕЛАЕТ BACKPRESSURE ВИДИМЫМ");

        Flux<Integer> numbers = Flux.range(1, 5)
                .doOnRequest(n -> System.out.println("[DEMAND] Flux.range увидел request(" + n + ")"))
                .doOnNext(value -> System.out.println("[ON_NEXT] Flux.range испускает " + value));

        ManualDemandSubscriber<Integer> subscriber = new ManualDemandSubscriber<>("flux-subscriber");
        numbers.subscribe(subscriber);

        for (int i = 0; i < 5; i++) {
            subscriber.request(1);
        }
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("================================================================================");
        System.out.println(title);
        System.out.println("================================================================================");
    }
}
