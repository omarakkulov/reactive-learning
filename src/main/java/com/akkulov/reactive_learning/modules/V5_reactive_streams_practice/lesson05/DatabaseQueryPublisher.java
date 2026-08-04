package com.akkulov.reactive_learning.modules.V5_reactive_streams_practice.lesson05;

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Cold Publisher одного результата: каждая подписка получает собственный
 * DatabaseQuerySubscription и собственное выполнение lookup после demand.
 */
public final class DatabaseQueryPublisher implements Publisher<UserRecord> {

    private final long userId;
    private final FakeReactiveDatabase database;

    public DatabaseQueryPublisher(long userId, FakeReactiveDatabase database) {
        this.userId = userId;
        this.database = Objects.requireNonNull(database, "database");
    }

    @Override
    public void subscribe(Subscriber<? super UserRecord> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");

        System.out.println("[SUBSCRIBE] Publisher получил нового Subscriber для userId=" + userId);
        subscriber.onSubscribe(new DatabaseQuerySubscription(userId, subscriber, database));
    }
}
