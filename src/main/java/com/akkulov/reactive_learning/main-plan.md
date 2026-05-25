# План Интенсива От Первого Урока

## Summary

Текущий первый урок оставляем как есть по содержанию: это фундаментальный “why reactive” урок. Он уже покрывает больше, чем обычное
введение: CPU/RAM/process/thread/scheduler, I/O-модели, thread-per-request, socket/selector/event loop, коммуникационные паттерны и Reactive
Streams на уровне идеи.

Оценка времени: если рассказывать `full-plan.md` спокойно, с пояснениями, картинками и короткими вопросами аудитории, это примерно **100-130
минут**. В формат **60-90 минут** он помещается, если вести его как интенсив: не уходить глубоко в спорные детали ОС, не раскрывать Reactive
Streams полностью и оставить практику короткой.

## Тайминг Первого Урока

Рекомендуемый тайминг на 90 минут:

| Блок                                           |     Время |
|------------------------------------------------|----------:|
| Цели урока и главная идея reactive             |     5 мин |
| Process / Thread / CPU / RAM / Scheduler       | 20-25 мин |
| Context switch и цена потоков                  |    10 мин |
| I/O models: sync, async, non-blocking          |    15 мин |
| Communication patterns и микросервисы          |    10 мин |
| Thread-per-request limit                       |    10 мин |
| Reactive Streams и reactive programming teaser |    10 мин |
| Socket / Selector / Event Loop                 |    15 мин |
| Итог и мост ко второму уроку                   |     5 мин |

Если делать практические `curl`-демо внутри первого урока, нужно закладывать **+15-20 минут**. Тогда полный урок станет ближе к **100-110
минутам**.

Практичный вариант: в первом уроке показать только `/thread` и одну пару blocking/non-blocking, а параллельные `curl` оставить как домашний
эксперимент или начало второго урока.

## Обновленная Структура Курса

### Урок 1. Blocking vs Non-Blocking. Зачем нужна реактивность

Источник: текущий `full-plan.md`.

Роль урока: объяснить, зачем вообще нужна реактивность.

Главный выход:

```text
Реактивность не ускоряет внешний мир.
Она меняет то, как приложение использует потоки во время ожидания.
```

Не углублять здесь:

- полный Reactive Streams protocol;
- операторы `Mono/Flux`;
- WebFlux request lifecycle;
- scheduler’ы Reactor в деталях.

### Урок 2. WebFlux Request Path

Роль: дополнить первый урок конкретным путем HTTP-запроса.

Темы:

- Netty server.
- Channel.
- ChannelPipeline.
- EventLoopGroup.
- Reactor Netty.
- Spring WebFlux поверх Netty.
- `HttpHandler`, `WebHandler`, `HandlerMapping`, `HandlerAdapter`.
- Controller возвращает `Mono/Flux`.
- Где HTTP response подписывается на publisher.

Главный выход:

```text
Человек может объяснить путь запроса в WebFlux так же уверенно,
как путь запроса через DispatcherServlet в Spring MVC.
```

### Урок 3. Reactive Streams Protocol

Роль: раскрыть то, что в первом уроке было teaser’ом.

Темы:

- `Publisher`.
- `Subscriber`.
- `Subscription`.
- `request(n)`.
- `onSubscribe`, `onNext`, `onError`, `onComplete`.
- Backpressure.
- Cold vs hot.
- Lazy execution.
- Почему без subscription ничего не происходит.
- Почему `Mono` это 0..1, а `Flux` это 0..N.

Главный выход:

```text
Человек понимает Reactor не как магию операторов,
а как поток сигналов по понятному протоколу.
```

### Урок 4. Mono/Flux Operators

Роль: научить писать рабочие reactive pipelines.

Темы:

- `map`.
- `flatMap`.
- `concatMap`.
- `flatMapSequential`.
- `filter`.
- `switchIfEmpty`.
- `defaultIfEmpty`.
- `then`.
- `zip`.
- `merge`.
- `concat`.
- `collectList`.
- `defer`.
- `fromCallable`.
- `onErrorResume`.
- `timeout`.

Главный выход:

```text
Человек понимает, какой оператор выбрать и почему.
```

### Урок 5. Threads, Schedulers, Blocking Boundaries

Роль: связать первый урок про потоки с реальным Reactor-кодом.

Темы:

- `reactor-http-nio-*`.
- `Schedulers.parallel()`.
- `Schedulers.boundedElastic()`.
- `Schedulers.single()`.
- `publishOn`.
- `subscribeOn`.
- Blocking adapter.
- Почему `block()` опасен.
- Почему `boundedElastic` это компромисс, а не волшебство.
- Reactor Context teaser.

Главный выход:

```text
Человек понимает, где выполняется его код,
и умеет не блокировать event loop.
```

### Урок 6. WebClient, Errors, Timeouts, Retries, Testing

Роль: перейти от учебных pipeline’ов к backend-интеграциям.

Темы:

- `WebClient`.
- `retrieve` vs `exchangeToMono`.
- HTTP error mapping.
- `timeout`.
- `retryWhen`.
- backoff.
- fallback.
- cancellation.
- `StepVerifier`.
- `WebTestClient`.

Главный выход:

```text
Человек умеет писать безопасный reactive HTTP-call:
с timeout, retry, fallback и тестами.
```

### Урок 7. Production-Like Reactive Flow

Роль: собрать все в один прикладной сценарий.

Пример:

```text
POST /orders
 -> validate
 -> find user
 -> check balance
 -> reserve product
 -> call payment
 -> save order
 -> return response
```

Темы:

- reactive service layer;
- orchestration через `Mono`;
- последовательные и параллельные шаги;
- domain errors vs technical errors;
- R2DBC vs JDBC teaser;
- observability teaser;
- code review reactive smells.

Главный выход:

```text
Человек видит, как из отдельных концепций собрать нормальный reactive backend flow.
```

## Логика Переходов

- Урок 1: Blocking vs Non-Blocking, зачем нужен reactive.
- Урок 2: как WebFlux принимает HTTP-запрос.
- Урок 3: как устроены сигналы Reactive Streams.
- Урок 4: как писать Mono/Flux pipeline.
- Урок 5: где выполняется код и как управлять потоками.
- Урок 6: как ходить наружу и обрабатывать ошибки.
- Урок 7: как собрать production-like flow.

## Assumptions

- Каждый урок рассчитан на **60-90 минут**.
- Первый урок остается содержательно таким, какой он есть сейчас.
- Если первый урок не помещается в 90 минут, его не режем по содержанию, а часть практики переносим в начало второго урока.
- Остальные уроки не повторяют фундамент первого, а надстраиваются над ним.
