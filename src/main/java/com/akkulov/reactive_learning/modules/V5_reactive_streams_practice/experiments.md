# Experiments. Lesson 05

Перед экспериментами запусти приложение:

```bash
./gradlew bootRun
```

Gradle-тесты для этой лекции отдельно не нужны. Здесь цель - смотреть HTTP responses и логи приложения.

## 1. Mono simple

Команда:

```bash
curl "http://localhost:8080/api/lesson-05/mono-simple"
```

Ожидаемый response:

```json
{"value":"Hello from Mono"}
```

Что смотреть в логах:

```text
[lesson-05:mono-simple] controller method invoked
[lesson-05:mono-simple] doOnSubscribe
[lesson-05:mono-simple] doOnRequest n=...
[lesson-05:mono-simple] doOnNext value=...
[lesson-05:mono-simple] doOnSuccess value=...
[lesson-05:mono-simple] doFinally signal=...
```

Вывод:

```text
Controller вернул Mono.
WebFlux подписался.
После subscription пошли сигналы.
```

Вопрос аудитории:

```text
Кто вызвал subscribe: мы в controller или WebFlux infrastructure?
```

## 2. Lazy execution

Команда:

```bash
curl "http://localhost:8080/api/lesson-05/lazy"
```

Ожидаемый response:

```json
{"value":"Lazy value"}
```

Что смотреть:

```text
controller method invoked
pipeline assembled, supplier has not executed yet
doOnSubscribe
doOnRequest
supplier executed
doOnNext
```

Вывод:

```text
Reactive chain сначала собирается.
Работа внутри Supplier начинается только после subscription.
```

Вопрос:

```text
Почему supplier executed не появился сразу при создании Mono?
```

## 3. Mono.delay

Команда:

```bash
curl "http://localhost:8080/api/lesson-05/delay"
```

Ожидаемый response примерно через секунду:

```json
{"value":"Delayed response"}
```

Что смотреть:

```text
controller method invoked
doOnSubscribe
doOnRequest
... пауза ...
doOnNext timerValue=0
doOnSuccess
doFinally
```

Вывод:

```text
Mono.delay не блокирует EventLoop.
Он планирует будущий сигнал.
HTTP response появится, когда Publisher испустит сигнал.
```

## 4. Mono empty

Команда:

```bash
curl -i "http://localhost:8080/api/lesson-05/empty"
```

Что смотреть:

```text
doOnSubscribe
doOnRequest
doOnSuccess value=null
doFinally
```

Вывод:

```text
Mono может завершиться без значения.
Mono = 0..1, а не "обязательно один объект".
```

## 5. Mono error

Команда:

```bash
curl -i "http://localhost:8080/api/lesson-05/error"
```

Что смотреть:

```text
doOnSubscribe
doOnRequest
doOnError error=Lesson 05 demo error
doFinally signal=onError
```

Вывод:

```text
onError - terminal signal.
После ошибки normal onComplete уже не будет.
```

Вопрос:

```text
Как WebFlux превращает error signal в HTTP response?
```

## 6. Flux streaming

Команда:

```bash
curl "http://localhost:8080/api/lesson-05/flux"
```

Что смотреть:

```text
doOnSubscribe
doOnRequest n=...
doOnNext value=LessonEvent[index=1, value=event-1]
doOnNext value=LessonEvent[index=2, value=event-2]
...
doOnComplete
doFinally
```

Вывод:

```text
Flux = 0..N элементов.
Каждый onNext может стать очередным элементом streaming response.
```

Вопрос:

```text
Почему для Flux тема request(n) и backpressure важнее, чем для Mono?
```

## 7. Cold publisher

Команда:

```bash
curl "http://localhost:8080/api/lesson-05/cold"
curl "http://localhost:8080/api/lesson-05/cold"
```

Что смотреть:

```text
new cold sequence for subscription=1
new cold sequence for subscription=2
```

Controller instance не создается заново на каждый HTTP-запрос. Счетчик общий для controller-а, поэтому повторные запросы показывают новые
subscription numbers. А `Flux.defer(...)` создает новую cold-последовательность для каждого subscriber.

Вывод:

```text
Cold Publisher начинает сценарий заново для каждого subscriber.
```

Аналогия:

```text
YouTube-видео: каждый зритель нажимает play и смотрит с начала.
```

## 8. Hot publisher explanation

Команда:

```bash
curl "http://localhost:8080/api/lesson-05/hot-demo-explanation"
```

Ожидаемый response:

```json
{
  "concept": "Hot Publisher",
  "analogy": "Radio broadcast: if you join later, you do not hear what already happened",
  "note": "Deep practice with Sinks will be covered later"
}
```

Вывод:

```text
Hot Publisher живет независимо от конкретного subscriber.
Поздний subscriber может пропустить прошлые события.
```

## Финальный контрольный вопрос

```text
Почему в controller мы обычно не вызываем subscribe() руками?
```

Ожидаемый ответ:

```text
Потому что WebFlux сам подписывается на Publisher,
чтобы связать signals с HTTP response lifecycle:
onNext -> body,
onError -> error response,
onComplete -> finish response.
```
