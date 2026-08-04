package com.akkulov.reactive_learning.modules.V5_reactive_streams_practice.lesson05;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Subscriber, который намеренно не вызывает request(...) в onSubscribe.
 * Благодаря этому на лекции видна разница между фактом подписки и demand.
 */
public final class ManualDemandSubscriber<T> implements Subscriber<T> {

    private final String name;
    private final List<T> values = new ArrayList<>();

    private Subscription subscription;
    private Throwable error;
    private boolean completed;

    public ManualDemandSubscriber(String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    @Override
    public void onSubscribe(Subscription newSubscription) {
        Objects.requireNonNull(newSubscription, "newSubscription");

        if (subscription != null) {
            System.out.println("[ON_SUBSCRIBE] " + name
                    + " уже имеет Subscription; новая связь отменяется");
            newSubscription.cancel();
            return;
        }

        subscription = newSubscription;
        System.out.println("[ON_SUBSCRIBE] " + name
                + " получил Subscription, но пока не запросил данные");
    }

    @Override
    public void onNext(T value) {
        values.add(value);
        System.out.println("[ON_NEXT] " + name + " получил значение: " + value);
        System.out.println();
    }

    @Override
    public void onError(Throwable throwable) {
        error = Objects.requireNonNull(throwable, "throwable");
        System.out.println("[ERROR] " + name + " получил onError: "
                + throwable.getClass().getSimpleName() + " — " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        completed = true;
        System.out.println("[COMPLETE] " + name + " получил onComplete()");
    }

    public void request(long n) {
        requireSubscription();
        System.out.println("[DEMAND] " + name + " вызывает request(" + n + ")");
        subscription.request(n);
    }

    public void cancel() {
        requireSubscription();
        System.out.println("[CANCEL] " + name + " вызывает cancel()");
        subscription.cancel();
    }

    public List<T> values() {
        return List.copyOf(values);
    }

    public Optional<Throwable> error() {
        return Optional.ofNullable(error);
    }

    public boolean isCompleted() {
        return completed;
    }

    private void requireSubscription() {
        if (subscription == null) {
            throw new IllegalStateException("Сначала Publisher должен вызвать onSubscribe");
        }
    }
}
