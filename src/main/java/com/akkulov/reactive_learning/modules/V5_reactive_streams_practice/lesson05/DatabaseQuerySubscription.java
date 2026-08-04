package com.akkulov.reactive_learning.modules.V5_reactive_streams_practice.lesson05;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Минимальная Mono-подобная Subscription для лекции.
 * <p>
 * Она поддерживает один lookup, положительный demand, ошибку при n <= 0, отмену.
 */
public final class DatabaseQuerySubscription implements Subscription {

    private final long userId;
    private final FakeReactiveDatabase database;
    private final AtomicReference<State> state = new AtomicReference<>(State.WAITING_FOR_DEMAND);
    private final AtomicReference<Subscriber<? super UserRecord>> subscriber;

    public DatabaseQuerySubscription(
            long userId,
            Subscriber<? super UserRecord> subscriber,
            FakeReactiveDatabase database
    ) {
        this.userId = userId;
        this.subscriber = new AtomicReference<>(Objects.requireNonNull(subscriber, "subscriber"));
        this.database = Objects.requireNonNull(database, "database");
    }

    @Override
    public void request(long n) {
        State currentState = state.get();

        if (currentState == State.CANCELLED || currentState == State.TERMINATED) {
            System.out.println("[DEMAND] request(" + n
                    + ") проигнорирован: subscription уже " + currentState);
            return;
        }

        System.out.println("[DEMAND] Subscription получила request(" + n + ") для userId=" + userId);

        if (n <= 0) {
            signalInvalidDemand(n);
            return;
        }

        if (!state.compareAndSet(State.WAITING_FOR_DEMAND, State.EMITTING)) {
            System.out.println("[DEMAND] повторный request(" + n + ") не запускает второй lookup");
            return;
        }

        Optional<UserRecord> result;
        try {
            result = database.executeLookup(userId);
        } catch (Throwable error) {
            signalLookupError(error);
            return;
        }

        if (state.get() == State.CANCELLED) {
            return;
        }

        result.ifPresent(this::signalNext);

        signalCompleteIfActive();
    }

    @Override
    public void cancel() {
        while (true) {
            State currentState = state.get();
            if (currentState == State.CANCELLED || currentState == State.TERMINATED) {
                System.out.println("[CANCEL] повторный cancel() не меняет состояние " + currentState);
                return;
            }
            if (state.compareAndSet(currentState, State.CANCELLED)) {
                subscriber.set(null);
                System.out.println("[CANCEL] subscription отменена; будущий lookup/сигналы запрещены");
                return;
            }
        }
    }

    private void signalInvalidDemand(long n) {
        if (!state.compareAndSet(State.WAITING_FOR_DEMAND, State.TERMINATED)) {
            return;
        }

        IllegalArgumentException error = new IllegalArgumentException(
                "Reactive Streams demand должен быть > 0, получено: " + n
        );
        Subscriber<? super UserRecord> target = subscriber.getAndSet(null);
        if (target != null) {
            System.out.println("[ERROR] request(" + n
                    + ") превращается в onError(IllegalArgumentException)");
            target.onError(error);
        }
    }

    private void signalLookupError(Throwable error) {
        if (!state.compareAndSet(State.EMITTING, State.TERMINATED)) {
            return;
        }

        Subscriber<? super UserRecord> target = subscriber.getAndSet(null);
        if (target != null) {
            System.out.println("[ERROR] lookup завершился onError: " + error.getMessage());
            target.onError(error);
        }
    }

    private void signalNext(UserRecord value) {
        if (state.get() != State.EMITTING) {
            return;
        }

        Subscriber<? super UserRecord> target = subscriber.get();
        if (target != null) {
            System.out.println("[ON_NEXT] Publisher передаёт " + value);
            target.onNext(value);
        }
    }

    private void signalCompleteIfActive() {
        if (!state.compareAndSet(State.EMITTING, State.TERMINATED)) {
            return;
        }

        Subscriber<? super UserRecord> target = subscriber.getAndSet(null);
        if (target != null) {
            System.out.println("[COMPLETE] Publisher больше не имеет значений и вызывает onComplete()");
            target.onComplete();
        }
    }

    private enum State {
        // Subscription передана Subscriber-у, но положительный request(n) ещё не получен.
        // Lookup не запущен, данные не отправляются.
        WAITING_FOR_DEMAND,

        // Первый положительный request(n) принят: выполняется lookup
        // и при наличии результата отправляется onNext.
        EMITTING,

        // Отправлен окончательный сигнал onComplete или onError.
        // Повторный request(n) больше не может запустить lookup или новые сигналы.
        TERMINATED,

        // Subscriber отменил связь через cancel(): новый lookup не запускается,
        // а новые сигналы не отправляются. Уже начатый синхронный lookup это не прерывает.
        // Повторный cancel() безопасен и не меняет состояние.
        CANCELLED
    }
}
