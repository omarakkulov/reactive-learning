# Лекция 4. Runtime-практика WebFlux Request Path

В третьей лекции мы разобрали теоретическую карту: как запрос из Reactor Netty попадает в Spring WebFlux, как находится controller,
как результат controller-а превращается в HTTP response.

В этой лекции мы делаем важный практический шаг: берем обычный request и смотрим на него в runtime через debugger и логи.

Главная цель занятия:

```text
Показать не просто "controller вызвался", а разобрать весь путь.

Простая схема:

bytes из сети
 -> Netty / Reactor Netty
 -> Spring WebFlux
 -> Controller
 -> Response Write
```

## 0. Что будет понятно после лекции

После занятия слушатель сможет объяснить:

- где Reactor Netty выполняет terminal subscribe и как subscription доходит до `Publisher` контроллера;
- как controller возвращает `Mono` или `Flux`, и кто пишет response в socket(outbound);
- почему `Mono` дает 0..1 элемент, а `Flux` может дать 0..N элементов;
- где Java-объект будущего ответа превращается в JSON или bytes;
- почему `writeWith(...)` - это не "просто записали строку", а reactive-запись body.

## 1. Большая карта с определениями

```text
Client bytes - байты HTTP request-а, пришедшие по TCP connection                                     |
 -> ByteBuf - буфер Netty, в котором лежат входящие или исходящие bytes                              |
 -> Netty Channel - объект Netty, представляющий одно сетевое соединение                             | NETTY
 -> EventLoop - поток/цикл Netty, который обрабатывает I/O events и короткие callbacks               |
 -> Reactor Netty - HTTP runtime система, построенная поверх Netty и Reactor                         |
 -> ReactorHttpHandlerAdapter - адаптер между Reactor Netty и Spring HttpHandler                     |----- BRIDGE TO SpringWebflux
 -> HttpHandler - минимальный контракт Spring: принять request и response, вернуть Mono<Void>        |
 -> HttpWebHandlerAdapter(HttpHandler) - адаптер, который создает ServerWebExchange                  |
 -> ServerWebExchange - коробка одного HTTP exchange: request + response обмен                       |
 -> FilteringWebHandler / DefaultWebFilterChain - framework chain без пользовательского фильтра    | SPRING WEBLUFX
 -> DispatcherHandler - центральный диспетчер WebFlux, похожий по роли на DispatcherServlet          |
 -> HandlerMapping - ищет, какой handler подходит под URL и HTTP method                              |
 -> HandlerAdapter - умеет вызвать найденный handler/controller method                               |
 -> Controller - наш учебный код, который возвращает Mono или Flux                                   |
 -> HandlerResult - коробка Spring-а с результатом вызова handler-а                                  |
 -> HandlerResultHandler - стратегия, которая знает, как писать этот результат в HTTP response       |
 -> HttpMessageWriter - кодирует Java object в JSON/SSE/bytes                                        |
 -> ServerHttpResponse.writeWith(...) - reactive-запись body в response                              |
 -> Reactor Netty / Netty WRITE-FLUSH - постановка bytes на запись и flush в socket                  |
```

![webflux-request-path-runtime.svg](webflux-request-path-runtime.svg)

## 1. Сразу практика. Запуск приложения

1) Для того чтобы понять, как SpringBoot Webflux приложение обрабатывает запрос, сначала необходимо разобраться с тем,
   как оно конфигурируется при запуске.
2) Собираются все стандартные + кастомные @Beans приложения
3) Создается framework WebFilterChain; пользовательских фильтров в учебном примере нет
4) Запускается приложение и ждет сетевые события

Важно проговорить:

```text
Controller не является началом request path.
Controller - это середина пути.

До него уже случились socket read,
адаптация в Spring WebFlux и поиск handler-а.

После него еще случатся subscription, signals, encoding и network response write.
```

## 2. Что есть в коде лекции

В V4 мы оставляем минимальный набор, чтобы не распылять внимание:

```text
RuntimeRequestPathController - controller с двумя endpoint-ами
Lesson04ProfileResponse - объект ответа для Mono endpoint
Lesson04StreamElement - элемент stream-а для Flux endpoint
Прямые [MONO]/[FLUX] log.info - assembly, subscription, demand и signals
```

Endpoint-ы:

```text
GET /api/lesson-04/mono-object?name=Omar
 -> returns Mono<Lesson04ProfileResponse>

GET /api/lesson-04/flux-elements
 -> returns Flux<Lesson04StreamElement>
 -> produces text/event-stream
```

Почему только два endpoint-а:

```text
Нам важно не показать много возможностей WebFlux, а внимательно пройти один request path.

Mono endpoint показывает обычный JSON object response.
Flux endpoint показывает response как sequence of elements.
```

## 4. Mono endpoint: что происходит концептуально

Команда:

```bash
curl "http://localhost:8080/api/lesson-04/mono-object?name=Omar"
```

Сначала request приходит из сети.

```text
Client
 -> TCP connection
 -> Netty Channel
 -> Reactor Netty
```

На этом этапе мы еще не в controller-е.

Дальше Reactor Netty передает request в Spring WebFlux:

```text
ReactorHttpHandlerAdapter#apply
 -> HttpHandler
 -> HttpWebHandlerAdapter#handle
 -> ServerWebExchange
```

`ServerWebExchange` можно объяснить так:

```text
Это коробка текущего HTTP-обмена.
Внутри нее лежит request, response и служебные attributes.
WebFlux дальше почти везде передает именно эту коробку.
```

Потом request проходит внутреннюю framework chain без пользовательского фильтра:

```text
FilteringWebHandler#handle
 -> DefaultWebFilterChain
 -> DispatcherHandler#handle
```

Пользовательский WebFilter намеренно не добавлен:

```text
Дополнительный doOnSubscribe фильтра выглядел бы как еще одна подписка
и отвлекал бы от terminal subscribe Reactor Netty и Publisher-а контроллера.
```

Потом `DispatcherHandler` ищет controller method:

```text
DispatcherHandler
 -> HandlerMapping
 -> HandlerAdapter
 -> RuntimeRequestPathController#monoObject
```

`HandlerMapping`:

```text
Ищет: кто умеет обработать GET /api/lesson-04/mono-object.
```

`HandlerAdapter`:

```text
Умеет вызвать найденный handler.
В нашем случае это method controller-а.
```

Теперь вызывается controller:

```java
Mono<Lesson04ProfileResponse> monoObject(String name)
```

Ключевая мысль:

```text
Controller не возвращает готовый JSON.
Controller возвращает Mono<Lesson04ProfileResponse>.
Mono - это Publisher, который сможет дать 0 или 1 значение после subscription.
```

В коде есть `Mono.fromSupplier(...)`.

Что это значит:

```text
Мы описали работу: "когда на Mono подпишутся, создай Lesson04ProfileResponse".

Но само создание объекта происходит не в момент объявления pipeline,
а в момент subscription.
```

Именно поэтому в логах важно различать:

```text
[MONO] controller: метод вызван, name=Omar | thread=reactor-http-nio-*
[MONO] return: возвращаем Mono без subscribe() | thread=reactor-http-nio-*
[MONO] doOnSubscribe: subscription дошла до Mono | thread=reactor-http-nio-*
[MONO] doOnRequest: request(n)=9223372036854775807 | thread=reactor-http-nio-*
[MONO] supplier: создаём Lesson04ProfileResponse после subscription и demand | thread=reactor-http-nio-*
```

## 5. HandlerResult: почему Spring не пишет response сразу

После вызова controller-а Spring получает не bytes и не JSON string, а Java-level result.

В нашем случае:

```text
Mono<Lesson04ProfileResponse>
```

Spring упаковывает это в `HandlerResult`.

Что это:

```text
HandlerResult - это коробка Spring-а с результатом вызова handler-а.
```

Зачем нужно:

```text
Spring должен хранить не только само returned value,
но и информацию о типе, metadata, binding context и о том,
как дальше этот результат можно обработать.
```

Как говорить на лекции:

```text
HandlerResult - это еще не HTTP response.
Это "результат controller-а в упаковке Spring-а".
```

Частая ошибка:

```text
Думать: controller вернул объект, значит response уже готов.
Нет. В WebFlux controller часто возвращает Publisher,
и response body появится только после subscription и signals.
```

## 6. HandlerResultHandler и момент subscription

Следующий слой - `HandlerResultHandler`.

Что это:

```text
Компонент WebFlux, который смотрит на HandlerResult и решает,
как превратить его в HTTP response.
```

Для `@RestController` и body response обычно важен `ResponseBodyResultHandler`.

Что он делает:

```text
Он понимает: это body response.
Значит нужно встроить returned Publisher в response-writing pipeline,
закодировать значения через HttpMessageWriter
и отдать body в ServerHttpResponse.
```

Аккуратная формулировка:

```text
Reactor Netty уже выполнил terminal subscribe на общем Mono<Void>.
Response-writing инфраструктура встраивает Publisher контроллера в этот pipeline,
после чего subscription распространяется до него через Reactor-операторы.
```

Что смотрим в логах:

```text
[MONO] doOnSubscribe: subscription дошла до Mono
[MONO] doOnRequest: request(n)=9223372036854775807
[MONO] doOnNext: response=Lesson04ProfileResponse[...]
```

Что это означает:

```text
До subscription Mono был описанием работы.
После subscription начался reactive conversation:
Subscriber запросил данные, Publisher выдал значение, затем завершился.
```

## 7. Encoding и writeWith

Когда `Mono` отдал `Lesson04ProfileResponse`, WebFlux должен превратить Java object в response body.

`HttpMessageWriter`:

```text
Это компонент, который умеет писать body в нужном формате.
Для обычного JSON response он использует JSON codec.
Для text/event-stream он пишет поток SSE events.
```

`EncoderHttpMessageWriter#write`:

```text
Практическая breakpoint-точка, где удобно показать:
Spring уже понял, что нужно кодировать body.
```

`ServerHttpResponse.writeWith(...)`:

```text
Это точка reactive-записи body в response.
Метод принимает Publisher с data buffers.
То есть body тоже пишется реактивно, а не как "собрали строку и одним махом отправили".
```

`Netty WRITE / FLUSH`:

```text
WRITE - поставить bytes на запись в outbound buffer.
FLUSH - попросить Netty реально протолкнуть накопленные bytes в socket.
```

Частая ошибка:

```text
Думать, что writeWith сразу синхронно отправил все bytes клиенту.
На самом деле write path зависит от reactive signals, socket readiness,
буферов и Netty event loop.
```

## 8. Flux endpoint: та же карта, но body состоит из нескольких элементов

Команда:

```bash
curl -N "http://localhost:8080/api/lesson-04/flux-elements"
```

Request path до controller-а почти такой же:

```text
Client
 -> Reactor Netty
 -> HttpWebHandlerAdapter
 -> ServerWebExchange
 -> framework WebFilterChain без пользовательского фильтра
 -> DispatcherHandler
 -> HandlerMapping
 -> HandlerAdapter
 -> RuntimeRequestPathController#fluxElements
```

Отличие начинается в returned value:

```java
Flux<Lesson04StreamElement>
```

Что такое `Flux` в рамках этой лекции:

```text
Flux - это Publisher, который может отдать 0..N элементов.
Не один объект, а последовательность.
```

В нашем endpoint-е:

```text
Flux.range(1, 5)
 -> создает пять номеров
 -> delayElements делает stream видимым во времени
 -> map превращает номер в Lesson04StreamElement
```

Важно сказать:

```text
delayElements не усыпляет event loop через Thread.sleep.
Он планирует появление следующего элемента во времени.

В логах может появиться thread вида parallel-*.
Это Reactor scheduler для таймера, а не blocking request-thread.
```

Что будет видно:

```text
[FLUX] doOnSubscribe: subscription дошла до Flux | thread=reactor-http-nio-*
[FLUX] doOnRequest: request(n)=1 | thread=reactor-http-nio-*
[FLUX] map: создаём Lesson04StreamElement, index=1 | thread=parallel-*
[FLUX] doOnNext: index=1 | thread=parallel-*
[FLUX] doOnRequest: request(n)=31 | thread=reactor-http-nio-*
[FLUX] map/doOnNext: index=2..5 | thread=parallel-*
...
[FLUX] doOnComplete | thread=parallel-*
[FLUX] doFinally: signal=onComplete | thread=parallel-*
```

Ключевая разница с Mono:

```text
Mono:
 -> максимум один onNext
 -> JSON object response

Flux:
 -> несколько onNext
 -> streaming response может отправлять элементы постепенно
```

## 9. Breakpoints для лекции

Для основного Mono-прохода:

```text
ReactorHttpHandlerAdapter#apply
HttpWebHandlerAdapter#handle
FilteringWebHandler#handle
DispatcherHandler#handle
AbstractHandlerMapping#getHandler
RequestMappingHandlerAdapter#handle
RuntimeRequestPathController#monoObject
return responsePublisher inside monoObject
DispatcherHandler#handleResult
ResponseBodyResultHandler#handleResult
doOnSubscribe inside monoObject
supplier inside Mono.fromSupplier
EncoderHttpMessageWriter#write
AbstractServerHttpResponse#writeWith
```

Для Flux-прохода:

```text
RuntimeRequestPathController#fluxElements
doOnSubscribe
doOnRequest
doOnNext
doOnComplete
EncoderHttpMessageWriter#write
AbstractServerHttpResponse#writeWith
```

Если в конкретной версии Spring класс или метод чуть отличается:

```text
Не застреваем.
Показываем ближайший аналог в call stack и опираемся на наши учебные логи.
Ценность лекции - не выучить приватные методы Spring,
а увидеть форму request path.
```

## 10. Что проговорить как главный вывод

Финальная формула:

```text
Controller returns Mono/Flux - controller отдает Publisher
 -> HandlerResult - Spring упаковывает результат handler-а
 -> HandlerResultHandler - выбирает стратегию записи в response
 -> subscription общего Mono<Void> доходит до Publisher-а контроллера
 -> HttpMessageWriter - Java objects превращаются в JSON/SSE/bytes
 -> ServerHttpResponse.writeWith - body записывается реактивно
 -> Reactor Netty / Netty - bytes уходят в socket
```

Очень важно:

```text
Мы сегодня не разбираем Reactive Streams глубоко.
Мы только увидели место, где эта модель становится важной:

Spring WebFlux получил Publisher от controller-а и встроил его в response pipeline.
Subscription от terminal boundary Reactor Netty дошла до Publisher-а,
чтобы signals могли стать HTTP response.
```

Мост к следующей лекции:

```text
На следующем занятии мы остановимся именно на этой цепочке:
"terminal subscribe → Subscription → request(n) → signals".

Разберем Publisher, Subscriber, Subscription, request(n),
signals, backpressure, Mono, Flux, cold и hot.
```
