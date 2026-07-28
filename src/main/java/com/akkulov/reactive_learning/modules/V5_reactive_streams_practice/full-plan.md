# Урок 5. Практика Reactive Streams

Эта лекция - следующий практический этап курса после runtime-разбора WebFlux request path.

До этого мы строили карту мира и в runtime посмотрели путь HTTP request/response:

```text
ОС - управляет процессами, потоками, памятью, sockets и I/O events
 -> потоки - единицы выполнения кода внутри процесса
 -> EventLoop - поток/цикл, который обрабатывает I/O-события и короткие callbacks
 -> Netty - низкоуровневый network framework поверх non-blocking I/O
 -> Reactor Netty - реактивная интеграция Netty с Publisher/Subscriber моделью Reactor
 -> Spring WebFlux request path - путь от HTTP-события до controller-а и записи response обратно пользователю
```

В предыдущей лекции мы руками увидели важную точку:

```text
Controller returns Mono/Flux - controller возвращает Publisher, а не готовые данные
 -> HandlerResult - объект Spring WebFlux с результатом вызова handler-а
 -> HandlerResultHandler - компонент, который умеет превратить HandlerResult в HTTP response
 -> WebFlux subscribes - инфраструктура подписывается на Publisher, чтобы получить сигналы
 -> signals - onSubscribe, onNext, onError, onComplete
 -> encode body - Java-объекты превращаются в JSON/SSE/bytes через codecs
 -> ServerHttpResponse.writeWith(...) - реактивная запись body в HTTP response
 -> Reactor Netty - слой, который связывает WebFlux response с Netty runtime
 -> Netty WRITE / FLUSH - постановка bytes на запись и фактический flush в socket
```

Теперь главный вопрос:

```text
Что значит "WebFlux подписывается на Publisher"?
```

Сегодня мы будем смотреть на endpoints, логи и сигналы. То есть мы не просто называем термины `Publisher`, `Subscriber`, `Subscription`,
`request(n)`, `onNext` и `onComplete`, а привязываем их к реальному HTTP request-response flow.

## 0. Что человек должен понять после урока

После урока слушатель должен суметь объяснить:

- почему `Mono` и `Flux` - это `Publisher`;
- что значит подписаться на `Publisher`;
- кто является `Subscriber` в учебной модели WebFlux;
- зачем нужен `Subscription`;
- почему `request(n)` связан с backpressure;
- что означают сигналы `onSubscribe`, `onNext`, `onError`, `onComplete`;
- почему reactive chain ленивый;
- почему без subscription ничего не происходит;
- почему `Mono` - это 0..1 элемент;
- почему `Flux` - это 0..N элементов;
- чем cold publisher отличается от hot publisher;
- как все это связано с тем, что WebFlux превращает результат controller-а в HTTP response.

Главная фраза урока:

```text
Controller возвращает Publisher.
WebFlux подписывается.
Сигналы Publisher становятся HTTP response.
```

## 1. Возвращаемся к лекциям 3 и 4

В третьей лекции мы разобрали теорию пути запроса внутри Spring WebFlux, а в четвертой увидели этот путь в runtime:

```text
Reactor Netty - HTTP runtime поверх Netty
 -> ReactorHttpHandlerAdapter - адаптер между Reactor Netty и Spring HttpHandler
 -> HttpHandler - минимальный контракт Spring для обработки HTTP request/response
 -> HttpWebHandlerAdapter - превращает request/response в ServerWebExchange
 -> ServerWebExchange - контейнер текущего HTTP exchange
 -> WebFilter chain - цепочка фильтров до handler-а
 -> DispatcherHandler - центральный диспетчер WebFlux
 -> HandlerMapping - ищет подходящий handler/controller method
 -> HandlerAdapter - вызывает найденный handler
 -> Controller returns Mono/Flux - controller возвращает Publisher
 -> HandlerResult - Spring упаковывает результат вызова handler-а
 -> HandlerResultHandler - выбирает стратегию записи результата в response
 -> response write path - кодирование и запись body обратно в сеть
```

Мы сказали, что controller method может вернуть:

```java
Mono<ResponseEntity<Map<String, String>>>
```

или:

```java
Flux<Event>
```

Но сам controller не пишет байты в socket. Он не вызывает напрямую `Channel.write(...)`. Он не вызывает `subscribe()` руками.

Он возвращает объект, который описывает будущую работу.

Вот это и есть мост:

```text
Лекции 3 и 4 объяснили, где WebFlux подписывается на Publisher.
Лекция 5 объясняет, что такое Publisher, что значит подписаться,
и какие сигналы после этого начинают течь.
```

## 2. Publisher на пальцах

`Publisher` - это не данные.

`Publisher` - это объект, который умеет когда-нибудь начать отдавать данные тому, кто на него подпишется.

Хорошая аналогия:

```text
Publisher похож не на книгу, лежащую у тебя в руках,
а на подписку на доставку книги.

Пока ты не оформил подписку, доставка не началась.
```

Если controller возвращает:

```java
Mono<ProfileResponse>
```

это не значит:

```text
ProfileResponse уже лежит в руках у Spring.
```

Это значит:

```text
Spring получил Publisher, который может дать ProfileResponse позже.
```

В WebFlux:

```text
Mono<ProfileResponse> - это Publisher.
Controller возвращает его Spring'у.
Spring подписывается, чтобы получить результат и записать HTTP response.
```

Практический endpoint:

```text
GET /api/lesson-05/mono-simple
```

Он возвращает `Mono<Map<String, String>>` и логирует события жизненного цикла.

## 3. Subscriber на пальцах

`Subscriber` - это сторона, которая говорит:

```text
Я хочу получать данные от Publisher.
```

В обычной жизни это похоже на человека, который подписался на доставку.

В reactive terms:

```text
Publisher - источник.
Subscriber - потребитель.
```

В учебной модели WebFlux важно сказать аккуратно:

```text
Subscriber - это не наш controller.
Subscriber находится внутри WebFlux/Reactor-инфраструктуры,
которая хочет получить значения из Mono/Flux и записать их в HTTP response.
```

В реальности внутри Reactor и Spring несколько внутренних subscribers/operators, но как mental model достаточно:

```text
WebFlux подписывается на Publisher,
чтобы превратить его сигналы в HTTP response.
```

Когда мы дергаем endpoint, в логах видим:

```text
doOnSubscribe
```

Это учебный след того, что подписка произошла.

## 4. Subscription на пальцах

`Subscription` - это связь между Publisher и Subscriber.

Важно: это не просто "факт подписки".

Это канал управления между потребителем и источником.

Аналогия:

```text
Ты подписался на доставку воды.
Subscription - это договор/пульт управления:
можно попросить следующую бутылку,
можно отменить доставку.
```

Технически через `Subscription` Subscriber может вызвать:

```text
request(n) - дай мне n элементов
cancel()   - больше не присылай
```

То есть Subscriber не просто пассивно сидит и принимает все подряд. У него есть способ сказать Publisher-у, сколько данных он готов принять.

Это ведет нас к `request(n)` и backpressure.

## 5. request(n) на пальцах

`request(n)` - это способ Subscriber сказать Publisher:

```text
Я готов принять n элементов.
```

Пример:

```text
Если Subscriber говорит request(1),
Publisher не должен сразу вывалить миллион элементов.
Он должен уважать demand.
```

`Demand` - это запрос на количество элементов.

Для `Mono` это почти незаметно:

```text
Mono максимум отдаст один элемент.
```

Но для `Flux` это принципиально:

```text
Flux может отдавать 10, 1000, миллион или бесконечное количество элементов.
```

Поэтому в streaming response `request(n)` и backpressure становятся важными: клиент, сеть или response-writing layer могут быть медленнее,
чем producer.

В наших endpoints мы добавляем:

```java
.doOnRequest(requested -> log.info("doOnRequest n={}", requested))
```

Чтобы увидеть demand в логах.

## 6. Signals: onSubscribe, onNext, onError, onComplete

Reactive Streams можно объяснить как разговор.

```text
Subscriber подписывается на Publisher.
Publisher говорит: onSubscribe(subscription)
Subscriber говорит: request(n)
Publisher отправляет: onNext(value)
Publisher завершает поток: onComplete()
```

Если случилась ошибка:

```text
Publisher отправляет: onError(error)
```

Важно:

```text
onError и onComplete - terminal signals.
После них поток завершен.
```

Нельзя после `onComplete` отправить новый `onNext`.

Нельзя после `onError` продолжить как будто ничего не случилось.

Связь с HTTP:

```text
onNext(body)   -> записать body
onError(error) -> сформировать error response
onComplete()   -> завершить response
```

Именно поэтому в лекциях 3 и 4 мы говорили:

```text
WebFlux подписывается на Publisher,
чтобы превратить signals в HTTP response.
```

## 7. Endpoint `mono-simple`: первый взгляд на сигналы

Endpoint:

```text
GET /api/lesson-05/mono-simple
```

Кодовая идея:

```java
return Mono.just(Map.of("value", "Hello from Mono"))
        .doOnSubscribe(...)
        .doOnRequest(...)
        .doOnNext(...)
        .doOnSuccess(...)
        .doFinally(...);
```

Что мы хотим увидеть:

```text
controller method invoked
doOnSubscribe
doOnRequest
doOnNext
doOnSuccess
doFinally
```

Как это рассказывать:

```text
Controller method был вызван через HandlerAdapter.
Метод вернул Mono.
Этот Mono стал частью HandlerResult.
HandlerResult - это "коробка Spring-а с результатом controller-а".
Внутри важен не только сам Mono, но и тип результата, annotations, возможные status/headers и контекст обработки.
HandlerResultHandler посмотрел на HandlerResult и выбрал стратегию, как превращать этот result в response.
Для этого WebFlux подписался на Mono.
После subscription мы видим сигналы.
```

Главный вывод:

```text
Mono в controller - это не "готовый response",
а Publisher, на который подпишется WebFlux.
```

## 8. Lazy execution

Reactive chain ленивый.

Это одна из самых важных идей, и ее нужно объяснить на пальцах.

Аналогия:

```text
Reactive chain похож на рецепт.
Пока никто не начал готовить, рецепт просто описывает шаги.
```

Создать `Mono` - не значит выполнить работу.

Endpoint:

```text
GET /api/lesson-05/lazy
```

Кодовая идея:

```java
Mono.fromSupplier(() -> {
    log.info("supplier executed");
    return Map.of("value", "Lazy value");
})
```

В controller мы логируем:

```text
controller method invoked
pipeline assembled, supplier has not executed yet
```

А внутри supplier:

```text
supplier executed
```

Что важно увидеть:

```text
controller method invoked - метод controller-а вызван
pipeline assembled        - reactive chain собран
supplier executed         - значение реально начали вычислять после subscription
```

Главная мысль:

```text
Создать Mono не значит выполнить работу.
Работа начинается при subscription.
```

Связь с request path из прошлых лекций:

```text
HandlerAdapter вызывает controller method.
Но выполнение Publisher начинается тогда,
когда WebFlux начинает обрабатывать HandlerResult и подписывается на Publisher.
```

## 9. Mono.delay и асинхронная граница

Endpoint:

```text
GET /api/lesson-05/delay
```

Кодовая идея:

```java
Mono.delay(Duration.ofSeconds(1))
```

`Mono.delay` не усыпляет EventLoop.

Он планирует сигнал на будущее.

Объяснение:

```text
HTTP request дошел до controller.
Controller вернул Mono.
WebFlux подписался.
Но значение еще не готово.
Mono.delay отдаст сигнал позже.
Когда сигнал приходит, response-writing layer продолжает работу.
```

Это идеальный мост к первой лекции:

```text
Поток не стоит и не ждет эту секунду.
Состояние запроса живет в reactive pipeline/framework state.
Когда сигнал приходит, обработка продолжается.
```

Главный вывод:

```text
HTTP response зависит от Publisher signals.
```

## 10. Mono: 0..1

`Mono` - это Publisher, который может завершиться одним из трех способов:

```text
1. дать один элемент и завершиться;
2. не дать ни одного элемента и завершиться;
3. завершиться ошибкой.
```

Endpoints:

```text
/api/lesson-05/mono-simple -> one value
/api/lesson-05/empty       -> no value
/api/lesson-05/error       -> error
```

Важно:

```text
Mono - это не "один объект".
Mono - это асинхронный сценарий, который может дать 0 или 1 значение.
```

`Mono.empty()` - это не `null`.

Это нормальное завершение без значения:

```text
onSubscribe
request(n)
onComplete
```

`Mono.error(...)` - это завершение ошибкой:

```text
onSubscribe
request(n)
onError
```

В HTTP это значит, что WebFlux должен выбрать, как превратить отсутствие значения или ошибку в response.

## 11. Flux: 0..N

`Flux` - это Publisher для последовательности элементов.

Эти элементы могут:

- прийти сразу;
- приходить постепенно;
- идти долго;
- завершиться успешно;
- завершиться ошибкой.

Endpoint:

```text
GET /api/lesson-05/flux
```

Он возвращает:

```java
Flux.range(1, 10)
    .delayElements(Duration.ofMillis(300))
```

И использует:

```java
produces = MediaType.TEXT_EVENT_STREAM_VALUE
```

Чтобы клиент видел streaming response.

Объяснение:

```text
Flux хорошо подходит для streaming response:
сервер может отправлять не один body целиком,
а последовательность элементов.
```

Связь с request path из прошлых лекций:

```text
Response-writing инфраструктура, выбранная через HandlerResultHandler, инициировала подписку на Flux.
Каждый onNext может стать очередным элементом response stream.
onComplete завершает stream.
```

Тут важно не застрять на слишком буквальной формулировке:

```text
HandlerResultHandler - это не обязательно единственный конкретный Subscriber.
Правильнее говорить так: HandlerResultHandler выбирает путь записи response,
а внутри этого пути WebFlux/Reactor-инфраструктура подписывается на Publisher.
```

## 12. Backpressure

Backpressure - это механизм не завалить потребителя данными.

На пальцах:

```text
Backpressure - это вежливый способ сказать:
"Не присылай быстрее, чем я могу обработать".
```

Аналогия:

```text
Кухня может готовить 100 блюд в минуту,
но официант может унести только 10.

Если кухня не учитывает скорость официанта,
тарелки начнут копиться и падать.
```

В Reactive Streams:

```text
Subscriber через request(n) сообщает demand.
Publisher должен учитывать demand.
```

В WebFlux:

```text
При streaming response это помогает не завалить сеть, память или клиента.
```

Важно не продавать backpressure как ускорение:

```text
Backpressure не ускоряет consumer.
Backpressure регулирует скорость producer-а.
```

Для `Mono` это почти незаметно.

Для `Flux` и streaming это становится одной из центральных идей.

## 13. Cold Publisher

Cold Publisher начинает работу заново для каждого subscriber.

На пальцах:

```text
Cold Publisher - это как YouTube-видео по запросу.
Каждый зритель нажал play и смотрит с начала.
```

Пример:

```java
Flux.range(1, 3)
```

Если два subscriber-а подписались, каждый получит:

```text
1, 2, 3
```

Endpoint:

```text
GET /api/lesson-05/cold
```

В коде используется `Flux.defer(...)`, чтобы показать:

```text
new cold sequence for subscription=1
```

Если открыть endpoint второй раз, будет создана новая последовательность.

Главная мысль:

```text
Cold publisher не "транслирует общий эфир".
Он начинает сценарий заново для каждого subscriber.
```

## 14. Hot Publisher

Hot Publisher живет независимо от конкретного subscriber.

На пальцах:

```text
Hot Publisher - это радиоэфир.
Если включил радио позже, ты не услышишь то, что уже прозвучало.
```

Еще пример:

```text
Live stream, event bus, биржевые котировки, поток кликов.
```

Если данные уже были испущены до подписки, поздний subscriber может их пропустить.

В этой лекции hot publisher лучше объяснить концептуально. Глубокую практику с `Sinks` можно оставить на отдельный блок позже, потому что
сейчас наша цель - понять базовую механику `Publisher`/`Subscriber`/signals и связать ее с WebFlux response.

Endpoint:

```text
GET /api/lesson-05/hot-demo-explanation
```

Он возвращает короткое JSON-объяснение концепции.

## 15. Cold vs Hot в одной таблице

| Concept | Cold | Hot |
| --- | --- | --- |
| Когда начинается | при подписке | может идти независимо |
| Что получает поздний subscriber | обычно с начала | только новые события |
| Аналогия | видео по запросу | радиоэфир |
| Пример | `Mono.fromSupplier`, `Flux.range` | live stream, event bus |

Короткая формула:

```text
Cold - каждый subscriber запускает сценарий для себя.
Hot - события живут своим потоком, subscriber подключается к уже идущему процессу.
```

## 16. Почему без subscription ничего не происходит

Финальный блок связывает всю лекцию.

```text
Publisher без Subscriber - это потенциальная работа.
```

Пример:

```java
Mono.fromSupplier(() -> {
    log.info("executed");
    return "value";
});
```

Если никто не подписался:

```text
executed не появится в логах
```

В WebFlux мы обычно не вызываем `subscribe()` руками.

Почему?

```text
Потому что WebFlux делает это за нас при обработке HandlerResult.
```

И это нормально:

```text
Controller returns Publisher.
WebFlux subscribes.
Signals become HTTP response.
```

Если же мы начинаем вызывать `subscribe()` внутри controller/service руками, мы часто отрываем работу от request lifecycle: ломаем
обработку ошибок, cancellation, контекст и response handling.

## 17. Главный вывод урока

Сегодня мы раскрыли сигналы, которые в предыдущих лекциях были видны как подписка и запись response.

Лекции 3-4:

```text
Controller returns Mono/Flux - controller отдает Publisher
 -> HandlerResult - Spring упаковывает результат handler-а
 -> HandlerResultHandler - выбирает, как писать результат в HTTP response
 -> WebFlux subscribes - инфраструктура начинает получать сигналы
 -> response write - сигналы превращаются в bytes и уходят через Reactor Netty/Netty
```

Пятая лекция:

```text
Publisher
 -> Subscriber
 -> Subscription
 -> request(n)
 -> onNext/onError/onComplete
 -> HTTP response
```

Главная фраза:

```text
Reactive Streams - это язык сигналов,
через который WebFlux превращает асинхронную работу controller-а
в HTTP response.
```

И финальная формула:

```text
Mono/Flux в controller - это не готовые данные.
Это Publisher, на который подпишется WebFlux,
чтобы получить сигналы и записать response.
```
