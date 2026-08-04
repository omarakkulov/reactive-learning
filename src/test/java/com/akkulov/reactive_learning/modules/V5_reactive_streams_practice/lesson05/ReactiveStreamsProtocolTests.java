package com.akkulov.reactive_learning.modules.V5_reactive_streams_practice.lesson05;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactiveStreamsProtocolTests {

    @Test
    void lookupDoesNotRunDuringAssemblyOrSubscribeWithoutDemand() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();
        Publisher<UserRecord> publisher = database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID);
        ManualDemandSubscriber<UserRecord> subscriber = new ManualDemandSubscriber<>("test-subscriber");

        assertEquals(0, database.lookupCount());

        publisher.subscribe(subscriber);

        assertEquals(0, database.lookupCount());
        assertTrue(subscriber.values().isEmpty());
        assertFalse(subscriber.isCompleted());
        assertTrue(subscriber.error().isEmpty());
    }

    @Test
    void positiveDemandEmitsOneValueAndOneCompletionOnly() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();
        ManualDemandSubscriber<UserRecord> subscriber = new ManualDemandSubscriber<>("test-subscriber");
        database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID).subscribe(subscriber);

        subscriber.request(1);
        subscriber.request(10);

        assertEquals(List.of(new UserRecord(42L, "Omar", true)), subscriber.values());
        assertTrue(subscriber.isCompleted());
        assertTrue(subscriber.error().isEmpty());
        assertEquals(1, database.lookupCount());
    }

    @Test
    void missingRowCompletesWithoutOnNext() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();
        ManualDemandSubscriber<UserRecord> subscriber = new ManualDemandSubscriber<>("missing");
        database.findUserById(FakeReactiveDatabase.MISSING_USER_ID).subscribe(subscriber);

        subscriber.request(1);

        assertTrue(subscriber.values().isEmpty());
        assertTrue(subscriber.isCompleted());
        assertTrue(subscriber.error().isEmpty());
        assertEquals(1, database.lookupCount());
    }

    @Test
    void lookupFailureProducesOnErrorWithoutOnComplete() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();
        ManualDemandSubscriber<UserRecord> subscriber = new ManualDemandSubscriber<>("failed");
        database.findUserById(FakeReactiveDatabase.FAILING_USER_ID).subscribe(subscriber);

        subscriber.request(1);

        assertTrue(subscriber.values().isEmpty());
        assertFalse(subscriber.isCompleted());
        assertInstanceOf(IllegalStateException.class, subscriber.error().orElseThrow());
        assertEquals(1, database.lookupCount());
    }

    @Test
    void nonPositiveDemandIsSignalledAsIllegalArgumentException() {
        for (long invalidDemand : List.of(0L, -1L)) {
            FakeReactiveDatabase database = new FakeReactiveDatabase();
            ManualDemandSubscriber<UserRecord> subscriber = new ManualDemandSubscriber<>("invalid");
            database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID).subscribe(subscriber);

            assertDoesNotThrow(() -> subscriber.request(invalidDemand));
            subscriber.request(1);

            assertInstanceOf(IllegalArgumentException.class, subscriber.error().orElseThrow());
            assertFalse(subscriber.isCompleted());
            assertTrue(subscriber.values().isEmpty());
            assertEquals(0, database.lookupCount());
        }
    }

    @Test
    void cancellationBeforeDemandPreventsLookupAndSignals() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();
        ManualDemandSubscriber<UserRecord> subscriber = new ManualDemandSubscriber<>("cancelled");
        database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID).subscribe(subscriber);

        subscriber.cancel();
        subscriber.cancel();
        subscriber.request(1);

        assertEquals(0, database.lookupCount());
        assertTrue(subscriber.values().isEmpty());
        assertFalse(subscriber.isCompleted());
        assertTrue(subscriber.error().isEmpty());
    }

    @Test
    void twoSubscribersCauseTwoIndependentColdLookups() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();
        Publisher<UserRecord> publisher = database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID);
        ManualDemandSubscriber<UserRecord> first = new ManualDemandSubscriber<>("first");
        ManualDemandSubscriber<UserRecord> second = new ManualDemandSubscriber<>("second");

        publisher.subscribe(first);
        publisher.subscribe(second);
        first.request(1);
        second.request(1);

        assertEquals(1, first.values().size());
        assertEquals(1, second.values().size());
        assertEquals(2, database.lookupCount());
    }

    @Test
    void customPublisherWorksInsideMonoOperatorPipeline() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();

        Mono<String> pipeline = Mono.from(database.findUserById(FakeReactiveDatabase.EXISTING_USER_ID))
                .filter(UserRecord::active)
                .map(UserRecord::name)
                .flatMap(database::loadGreeting)
                .switchIfEmpty(Mono.defer(database::anonymousGreeting));

        StepVerifier.create(pipeline)
                .expectNext("Привет, Omar!")
                .verifyComplete();

        assertEquals(1, database.lookupCount());
    }

    @Test
    void emptyCustomPublisherActivatesLazyFallback() {
        FakeReactiveDatabase database = new FakeReactiveDatabase();
        AtomicBoolean fallbackActivated = new AtomicBoolean();

        Mono<String> pipeline = Mono.from(database.findUserById(FakeReactiveDatabase.MISSING_USER_ID))
                .map(UserRecord::name)
                .flatMap(database::loadGreeting)
                .switchIfEmpty(Mono.defer(() -> {
                    fallbackActivated.set(true);
                    return database.anonymousGreeting();
                }));

        assertFalse(fallbackActivated.get());

        StepVerifier.create(pipeline)
                .expectNext("Привет, незнакомец!")
                .verifyComplete();

        assertTrue(fallbackActivated.get());
    }

    @Test
    void fluxEmitsOnlyAsManualDemandArrives() {
        ManualDemandSubscriber<Integer> subscriber = new ManualDemandSubscriber<>("flux");
        Flux.range(1, 5).subscribe(subscriber);

        assertTrue(subscriber.values().isEmpty());

        subscriber.request(1);
        assertEquals(List.of(1), subscriber.values());
        assertFalse(subscriber.isCompleted());

        subscriber.request(2);
        assertEquals(List.of(1, 2, 3), subscriber.values());
        assertFalse(subscriber.isCompleted());

        subscriber.request(2);
        assertEquals(List.of(1, 2, 3, 4, 5), subscriber.values());
        assertTrue(subscriber.isCompleted());
    }
}
