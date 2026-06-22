# Lesson 04. WebFlux Practice + Reactive Streams

## Главная идея

```text
Controller returns Publisher.
WebFlux subscribes.
Signals become HTTP response.
```

## Возвращаемся к лекции 3

```text
Controller returns Mono/Flux
 -> HandlerResult
 -> HandlerResultHandler
 -> WebFlux subscribes
 -> signals
 -> encode body
 -> ServerHttpResponse.writeWith(...)
 -> Reactor Netty
 -> Netty WRITE / FLUSH
```

## Publisher

```text
Publisher - это не данные.
Publisher - это объект, который умеет начать отдавать данные тому,
кто на него подпишется.
```

Аналогия:

```text
не книга в руках,
а подписка на доставку книги
```

## Subscriber

```text
Subscriber - потребитель сигналов Publisher.
```

В учебной модели WebFlux:

```text
Subscriber находится внутри WebFlux/Reactor-инфраструктуры,
которая пишет HTTP response.
```

## Subscription

```text
Subscription - связь между Publisher и Subscriber.
```

Через нее Subscriber может:

```text
request(n)
cancel()
```

## request(n)

```text
request(n) = "я готов принять n элементов"
```

Для `Mono` почти незаметно.

Для `Flux` и streaming response принципиально.

## Signals

```text
onSubscribe -> связь создана
request(n)   -> demand
onNext       -> элемент
onError      -> ошибка, terminal signal
onComplete   -> успешно завершено, terminal signal
```

Связь с HTTP:

```text
onNext(body)   -> write body
onError(error) -> error response
onComplete()   -> finish response
```

## Endpoints

```text
GET /api/lesson-04/mono-simple
GET /api/lesson-04/lazy
GET /api/lesson-04/delay
GET /api/lesson-04/flux
GET /api/lesson-04/empty
GET /api/lesson-04/error
GET /api/lesson-04/cold
GET /api/lesson-04/hot-demo-explanation
```

## Lazy execution

```text
Reactive chain похож на рецепт.
Пока никто не начал готовить, рецепт просто описывает шаги.
```

```text
Создать Mono не значит выполнить работу.
Работа начинается при subscription.
```

## Mono

```text
Mono = Publisher на 0..1 элемент
```

Варианты:

```text
one value
empty
error
```

## Flux

```text
Flux = Publisher на 0..N элементов
```

Подходит для:

```text
streaming response
events
sequences
```

## Backpressure

```text
Backpressure = "не присылай быстрее, чем я могу обработать"
```

Технически:

```text
Subscriber через request(n) сообщает demand.
Publisher учитывает demand.
```

## Cold vs Hot

| Concept | Cold | Hot |
| --- | --- | --- |
| Когда начинается | при подписке | может идти независимо |
| Поздний subscriber | обычно с начала | только новые события |
| Аналогия | YouTube/video on demand | радиоэфир/live stream |
| Пример | `Mono.fromSupplier`, `Flux.range` | event bus, live events |

## Итог

```text
Reactive Streams - это язык сигналов,
через который WebFlux превращает асинхронную работу controller-а
в HTTP response.
```
