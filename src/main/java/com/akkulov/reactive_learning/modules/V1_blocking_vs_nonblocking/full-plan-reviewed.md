# День 1. Blocking vs Non-Blocking

---

## 0. Главная идея первого урока

Первый урок не должен начинаться с операторов `map`, `flatMap`, `zip`, `then`, `retryWhen` и даже не должен начинаться с глубокого
разбора `Mono` и `Flux`.

Сначала человеку нужно собрать внутреннюю модель:

```text
Почему blocking-подход имеет предел
        ↓
Что происходит с Java-потоком во время ожидания I/O
        ↓
Почему поток — не бесплатный ресурс
        ↓
Как ОС планирует потоки
        ↓
Чем async отличается от non-blocking
        ↓
Зачем нужны socket, selector и event loop
        ↓
Почему Netty/WebFlux/Reactor строятся вокруг неблокирующей модели
```

Главная мысль дня:

```text
Reactive programming не ускоряет внешний мир.

Если база данных отвечает 300 ms, реактивность не превратит эти 300 ms в 3 ms.

Но reactive/non-blocking модель может помочь приложению не удерживать поток во время ожидания этих 300 ms.
```

То есть реактивность — это не магическое ускорение БД, сети или внешнего сервиса. Это другая модель использования потоков, памяти и I/O 
операций во время ожидания.

---

## 1. Что человек должен понять после первого урока

В конце первого урока слушатель должен уметь простыми словами объяснить:

- что происходит, когда мы запускаем Java-приложение;
- чем процесс отличается от потока;
- почему поток не бесплатный;
- что делает scheduler операционной системы;
- что означают статусы потоков `RUNNING`, `RUNNABLE`, `WAITING` на уровне идеи;
- почему blocking I/O занимает поток ожиданием;
- почему `async` и `non-blocking` — не одно и то же;
- почему `CompletableFuture` или отдельный thread pool не делают код автоматически non-blocking;
- что такое socket на уровне идеи;
- зачем нужны selector и event loop;
- почему event loop нельзя блокировать;
- почему WebFlux нельзя писать как обычный Spring MVC-код, просто завернув результат в `Mono`;
- какую проблему решают Reactive Streams и backpressure;
- где реактивность полезна, а где она может быть лишней;

---

## 2. Начинать лучше не с ОС, а с backend-сценария

Представим сервис:

```text
GET /clients/123/profile
```

Чтобы вернуть ответ, сервису нужно:

1. сходить в БД за клиентом;
2. сходить в account-service за счетами;
3. сходить в limit-service за лимитами;
4. сходить в transaction-service за последними операциями;
5. собрать JSON-ответ.

В blocking-модели это часто выглядит так:

```text
HTTP request
   ↓
worker thread
   ↓
Controller
   ↓
Service
   ↓
DB call             ← поток ждет
   ↓
HTTP account call   ← поток ждет
   ↓
HTTP limits call    ← поток ждет
   ↓
Response
```

Или визуально:

```text
JAVA-thread-1:

      [получил request]
             ↓
     [немного Java-кода]
             ↓
[......... ждет БД .........]
             ↓
     [немного Java-кода]
             ↓
[......... ждет HTTP .......]
             ↓
     [немного Java-кода]
             ↓
     [вернул response]
```

Главный вопрос текущего этапа:

```text
Что значит "поток ждет" с точки зрения операционной системы?
```

Чтобы ответить на этот вопрос, нам нужна минимальная модель ОС: процесс, поток, scheduler, состояния потока и I/O.

---

## 3. Что происходит при запуске Java-приложения

Предположим, мы разработали Java-приложение и упаковали его в JAR-файл.

Когда мы пишем:

```bash
java -jar MyApp.jar
```

```text
То операционная система создает JVM процесс. Внутри этого процесса, JVM загружает наше Java-приложение и запускает главный Java-поток.
```

```text
shell
  ↓
exec java
  ↓
OS создает JVM process
  ↓
JVM читает MyApp.jar
  ↓
JVM загружает classes
  ↓
JVM запускает main Java thread
  ↓
public static void main(...)
```

---

## 4. Процесс

Процесс — это запущенный экземпляр программы с собственным виртуальным адресным пространством и ресурсами ОС.

Для Java-приложения процессом будет не сам JAR, а процесс JVM.

У процесса есть:

- виртуальное адресное пространство;
- загруженные библиотеки;
- открытые файловые дескрипторы;
- сокеты;
- память, выделенная под heap/native memory/thread stacks;
- один или несколько потоков выполнения.

Упрощенная схема JVM-процесса:

```text
JVM Process
┌──────────────────────────────────────────────┐
│ Virtual address space                        │
│                                              │
│  Java Heap                                   │
│  Metaspace                                   │
│  Code Cache                                  │
│  Native Memory                               │
│  Thread-1 Stack                              │
│  Thread-2 Stack                              │
│  Thread-3 Stack                              │
│                                              │
│  Open files / sockets / descriptors          │
└──────────────────────────────────────────────┘
```

Главная формула:

```text
Процесс — единица ресурсов.
```

То есть процесс отвечает на вопрос:

```text
Какие ресурсы выделены запущенной программе?
```

---

## 5. Поток

Поток — это единица выполнения внутри процесса.

Поток отвечает на вопрос:

```text
Кто прямо сейчас выполняет инструкции?
```

У процесса может быть один поток или много потоков.

```text
JVM Process
┌──────────────────────────────────────────────┐
│ Shared Java Heap                             │
│                                              │
│ Thread-1 → stack + execution context         │
│ Thread-2 → stack + execution context         │
│ Thread-3 → stack + execution context         │
│                                              │
└──────────────────────────────────────────────┘
```

У каждого platform thread есть:

- свой stack;
- свой execution context;
- состояние для scheduler-а;
- связь с underlying OS thread.

Важно:

```text
Heap общий для потоков процесса.
Stack свой у каждого потока.
```

Формула:

```text
Процесс — единица ресурсов.
Поток — единица выполнения.
```

---

## 6. Java platform threads и virtual threads

Для классического Java-приложения долгое время было удобно говорить:

```text
Java Thread ≈ OS Thread
```

Но в современных Java-версиях нужно уточнение.

В Java есть два важных вида потоков:

```text
Platform thread
  ↓
тонкая обертка над OS thread

Virtual thread
  ↓
легковесный поток уровня JDK,
который не удерживает отдельный OS thread на весь lifetime
```

В этом уроке мы в основном говорим о platform threads, потому что:

- event loop threads Netty — это реальные platform/OS threads;
- servlet worker threads в классической модели — тоже platform threads;
- проблема thread-per-request исторически связана именно с большим количеством platform threads.

Virtual threads важны, и их обязательно нужно сравнить с WebFlux отдельно, но не в самом начале. Иначе курс смешает две разные модели
масштабирования:

```text
WebFlux / Netty:
  small number of event-loop platform threads + non-blocking I/O

Virtual threads:
  many lightweight Java threads + blocking-style code
```

Короткая фраза для урока:

```text
Пока что под Java Thread мы будем понимать platform thread.
К virtual threads вернемся позже, когда сравним подходы.
```

---

## 7. Что есть у потока

Для реактивного курса не нужно глубоко уходить в архитектуру CPU, но полезно дать минимальную модель.

У потока есть:

- instruction pointer;
- stack;
- registers/context;
- состояние для scheduler-а.

### 7.1. Instruction pointer

Instruction pointer — это указатель на следующую инструкцию, которую должен выполнить CPU.

Упрощенная аналогия:

```text
Instruction pointer — это закладка:
"с какой инструкции продолжить выполнение".
```

Когда поток выполняется на CPU, это состояние находится в регистрах процессора. Когда ОС снимает поток с CPU, она сохраняет его execution
context в память, чтобы потом восстановить выполнение с того же места.

Для курса достаточно такой формулировки:

```text
Когда поток уходит с CPU,
ОС сохраняет его состояние.

Когда поток возвращается на CPU,
ОС восстанавливает это состояние,
и выполнение продолжается с нужного места.
```

Не нужно в основном тексте глубоко объяснять TCB, регистры и физические ячейки CPU. Это можно вынести в appendix.

### 7.2. Stack

Stack — это область памяти потока, где живут:

- frames вызовов методов;
- локальные переменные;
- адреса возврата;
- часть служебной информации выполнения.

Если методы вызывают друг друга бесконечно, стек может закончиться:

```java
void recursive() {
    recursive();
}
```

Результат:

```text
java.lang.StackOverflowError
```

Важно для нас:

```text
Каждый platform thread требует память под stack.
```

Но не нужно говорить, что это всегда строго 1 МБ. Размер зависит от JVM, ОС, архитектуры и параметра `-Xss`.

Правильная формулировка:

```text
Каждый platform thread требует память под stack.
На практике это часто сотни килобайт или мегабайты на поток,
но точное значение зависит от JVM/ОС/настроек.
```

### 7.3. Registers / execution context

Регистры — это маленькие и очень быстрые ячейки внутри CPU, с которыми процессор работает напрямую.

Когда ОС переключает выполнение с одного потока на другой, она должна сохранить существенную часть состояния текущего потока и восстановить
состояние следующего.

Это называется context switch.

---

## 8. Scheduler операционной системы

Scheduler — это часть ОС, которая решает, какой поток получит CPU.

Упрощенная модель:

```text
RUNNABLE threads queue
        ↓
    Scheduler
        ↓
CPU executes selected thread
```

Scheduler учитывает разные факторы:

- приоритеты;
- квант времени;
- состояние потока;
- загруженность CPU;
- affinity к ядрам;
- fairness;
- системные политики планирования.

Для первого урока достаточно трех состояний:

```text
RUNNING
  поток прямо сейчас выполняется на CPU

RUNNABLE / READY
  поток готов выполняться, но ждет CPU

WAITING / BLOCKED
  поток не может продолжить, пока не произойдет событие
```

Схема:

```text
            scheduler gives CPU
RUNNABLE ───────────────────────▶ RUNNING
   ▲                                │
   │                                │ blocking I/O / sleep / wait
   │                                ▼
   └────── event completed ◀───── WAITING
```

Важно сказать честно:

```text
Это упрощенная модель.
Реальные состояния в ОС и Java Thread.State отличаются,
но для понимания blocking I/O нам достаточно этой схемы.
```

---

## 9. Context switch

Context switch — это переключение CPU с выполнения одного потока на выполнение другого.

Упрощенно:

```text
Thread A is running
  ↓
OS saves Thread A context
  ↓
OS loads Thread B context
  ↓
Thread B continues running
```

Почему это важно?

Потому что большое количество потоков означает:

- больше stack memory;
- больше scheduling overhead;
- больше context switches;
- хуже cache locality;
- сложнее держать высокую конкурентную нагрузку.

Не нужно говорить, что context switch всегда катастрофически дорогой. Современные ОС делают это быстро. Но если потоков очень много и они
постоянно конкурируют за CPU, накладные расходы начинают быть заметными.

---

## 10. Blocking I/O: что значит, что поток ждет

Теперь возвращаемся к backend-сценарию.

Допустим, код делает blocking HTTP-вызов:

```java
String response = blockingHttpClient.call(request);
```

Концептуально происходит следующее:

```text
Thread sends request
  ↓
Thread cannot continue without response
  ↓
OS marks thread as waiting for I/O
  ↓
CPU can be used by another runnable thread
  ↓
Network response arrives
  ↓
OS wakes the waiting thread
  ↓
Thread becomes runnable
  ↓
Scheduler eventually gives it CPU
  ↓
Java code continues
```

Визуально:

```text
Java Thread-17

RUNNING
  |
  | send HTTP request
  v
WAITING for socket data
  |
  | response arrived
  v
RUNNABLE
  |
  | scheduler gives CPU
  v
RUNNING
  |
  | parse response
  v
continue Java code
```

Главная аккуратная формулировка:

```text
Когда поток заблокирован на I/O,
он не выполняет полезный Java-код.

CPU не обязан простаивать целиком:
ОС может дать CPU другому runnable-потоку.

Но сам waiting thread никуда не исчезает:
он занимает stack memory,
имеет состояние в ОС/JVM
и позже должен быть снова запланирован.
```

---

## 11. Почему thread-per-request имеет предел

В классической servlet-модели удобно думать так:

```text
request → worker thread → controller → service → repository/client → response
```

Один входящий запрос занимает worker thread.

Пока нагрузка небольшая, это отличная модель:

- простая;
- последовательная;
- понятная;
- хорошо ложится на обычный Java-код;
- легко отлаживается;
- естественно читается сверху вниз.

Проблема появляется, когда внутри обработки запроса много ожидания:

- запрос в базу данных;
- HTTP-вызов в другой микросервис;
- обращение к брокеру;
- чтение файла;
- ожидание Redis/Kafka/external API.

Если все эти операции blocking, поток ждет.

Еще раз:

```text
CPU не обязан простаивать целиком.
Но request-thread занят ожиданием
и не может в это время обслужить другой запрос.
```

Если одновременно пришло 1000 запросов, и каждый ждет внешний сервис:

```text
Request-1    → Thread-1    → WAITING
Request-2    → Thread-2    → WAITING
Request-3    → Thread-3    → WAITING
...
Request-1000 → Thread-1000 → WAITING
```

Проблема не в том, что blocking-модель плохая сама по себе. Проблема в том, что при большом количестве concurrent I/O-waiting операций нам
нужно много platform threads.

А много platform threads означает:

```text
много потоков
  ↓
много stack memory
  ↓
больше работы scheduler-а
  ↓
больше context switching
  ↓
выше риск latency spikes
  ↓
сложнее держать высокий concurrency
```

---

## 12. CPU-bound и I/O-bound работа

Чтобы понять, где реактивность полезна, нужно различать два типа работы.

### 12.1. CPU-bound

CPU-bound работа — это работа, где основное узкое место CPU.

Примеры:

- посчитать хэш;
- выполнить тяжелую криптографию;
- обработать большой JSON;
- сжать изображение;
- выполнить сложный алгоритм;
- сделать тяжелый mapping миллионов объектов.

Здесь non-blocking сам по себе не решает проблему.

Если CPU занят вычислениями, нам нужны:

- parallelism;
- отдельные scheduler-ы;
- ограничение concurrency;
- очереди;
- rate limiting;
- иногда отдельный сервис/воркер.

### 12.2. I/O-bound

I/O-bound работа — это работа, где основное узкое место внешний мир.

Примеры:

- сходить в БД;
- отправить HTTP-запрос;
- получить данные из Redis;
- дождаться Kafka;
- прочитать файл;
- дождаться ответа от внешнего сервиса.

Здесь приложение часто большую часть времени не считает, а ждет.

Главный вопрос:

```text
Если мы все равно ждем внешний мир,
можно ли не удерживать поток во время ожидания?
```

Именно здесь non-blocking модель становится интересной.

---

## 13. Async, non-blocking и reactive — это разные вещи

Эти понятия часто смешивают, поэтому их нужно развести в первом уроке.

### 13.1. Synchronous + blocking

Код вызывает операцию и ждет результат в том же потоке.

```java
String result = client.call();
```

Схема:

```text
Thread-1: call external service
Thread-1: waits
Thread-1: gets response
Thread-1: continues
```

Аналогия:

```text
Я звоню в парикмахерскую и держу трубку,
пока мне не ответят.
Я занят ожиданием.
```

### 13.2. Async, но все еще blocking внутри

Мы можем вынести blocking-вызов в другой поток:

```java
CompletableFuture.supplyAsync(() ->blockingClient.

call(),executor)
```

Для вызывающего потока это выглядит async: он не ждет.

Но где-то в executor-е другой поток все равно блокируется.

Схема:

```text
Thread-1: delegates work and continues
Thread-9: performs blocking call and waits
```

Аналогия:

```text
Я попросил друга позвонить в парикмахерскую.
Я свободен, но друг теперь занят ожиданием.
```

Вывод:

```text
Async не гарантирует non-blocking.
Async может просто переложить блокировку на другой поток.
```

### 13.3. Non-blocking

Non-blocking означает, что поток не ждет данные внутри blocking-вызова.

Он регистрирует интерес к событию и освобождается.

```text
start operation
register callback/interest
return thread to event loop
continue when event arrives
```

Аналогия:

```text
Я оставил номер телефона.
Меня уведомят, когда мастер освободится.
Я не держу линию и не заставляю друга ждать.
```

### 13.4. Reactive

Reactive programming — это не просто `async` и не просто `non-blocking`.

Для backend-разработчика полезная формула такая:

```text
Reactive programming =
  asynchronous data flow
  + non-blocking execution model
  + composition of streams
  + backpressure
```

Reactive Streams добавляют важный контракт:

```text
Потребитель может управлять тем,
сколько данных он готов принять.
```

---

## 14. Типы коммуникаций

Реактивность полезна не только для классического request-response.

Есть несколько моделей общения.

### 14.1. Request → Response

Классика:

```text
Client ── request ──▶ Server
Client ◀─ response ─ Server
```

Пример:

```text
GET /clients/123
```

### 14.2. Request → Streaming Response

Клиент отправляет один запрос, а сервер возвращает поток событий.

```text
Client ── request ─────────▶ Server
Client ◀─ event-1 ───────── Server
Client ◀─ event-2 ───────── Server
Client ◀─ event-3 ───────── Server
```

Примеры:

- Server-Sent Events;
- поток статусов заказа;
- обновления курьера на карте;
- realtime dashboard;
- поток логов.

### 14.3. Streaming Request → Response

Клиент отправляет поток данных, сервер в конце возвращает один ответ.

```text
Client ── event-1 ─────────▶ Server
Client ── event-2 ─────────▶ Server
Client ── event-3 ─────────▶ Server
Client ◀─ final response ── Server
```

Примеры:

- загрузка большого файла кусками;
- поток телеметрии с устройства;
- пачка событий для агрегации.

### 14.4. Bidirectional Streaming

Обе стороны одновременно обмениваются потоками сообщений.

```text
Client ── event-1 ─────────▶ Server
Client ◀─ event-A ───────── Server
Client ── event-2 ─────────▶ Server
Client ◀─ event-B ───────── Server
```

Примеры:

- чаты;
- collaborative editing;
- игровые события;
- live-сессии;
- gRPC bidirectional streaming.

Важная мысль:

```text
Обычный request-response не исчезает.
Но современные системы часто требуют streaming-моделей,
и reactive-подход хорошо ложится на такие сценарии.
```

---

## 15. Reactive Streams

Теперь можно ввести Reactive Streams.

Reactive Streams — это спецификация для асинхронной обработки потоков данных с неблокирующим backpressure.

Минимальная модель:

```text
Publisher
   ↓ subscribe
Subscriber
   ↓ request(n)
Publisher
   ↓ onNext(value)
   ↓ onNext(value)
   ↓ onComplete()
```

Или при ошибке:

```text
Publisher
   ↓ onError(error)
```

Четыре ключевые сущности:

```text
Publisher
  источник данных

Subscriber
  потребитель данных

Subscription
  связь между Publisher и Subscriber,
  через которую Subscriber запрашивает данные

Processor
  одновременно Subscriber и Publisher
```

Самая важная идея — `request(n)`.

```text
Subscriber говорит Publisher:
"Я готов принять N элементов".
```

Пример:

```text
Subscriber: request(3)
Publisher:  onNext(1)
Publisher:  onNext(2)
Publisher:  onNext(3)

Subscriber: request(2)
Publisher:  onNext(4)
Publisher:  onNext(5)
```

Это и есть идея backpressure:

```text
Потребитель может контролировать скорость,
с которой производитель отдает данные.
```

Важно: в первом уроке не нужно глубоко разбирать все правила спецификации. Достаточно объяснить, какую проблему решает контракт.

---

## 16. Reactor, Mono и Flux — пока только на уровне идеи

Spring WebFlux по умолчанию использует Project Reactor как reactive-библиотеку.

Reactor дает два базовых типа:

```text
Mono<T>
  0 или 1 элемент

Flux<T>
  0..N элементов
```

В первом уроке не нужно глубоко разбирать операторы. Достаточно дать ментальную модель:

```text
Mono/Flux — это не значение.
Mono/Flux — это описание асинхронного потока сигналов.
```

Например:

```java
Mono<Client> clientMono = clientRepository.findById(id);
```

Это не значит:

```text
Client уже лежит внутри Mono.
```

Это значит:

```text
Есть описание операции,
которая при подписке может асинхронно дать Client,
завершиться пусто
или завершиться ошибкой.
```

Сигналы:

```text
onSubscribe
onNext
onComplete
onError
```

Для первого дня достаточно:

```java
Mono.just(...)
Mono.

delay(...)

map(...)

flatMap(...)

doOnSubscribe(...)

doOnNext(...)

doFinally(...)

thenReturn(...)
```

Но цель дня — не операторы. Цель дня — понять, зачем эта модель вообще нужна.

---

## 17. Сетевка под капотом: socket

Теперь можно переходить к сетевой модели.

### 17.1. IP, port, socket

Упрощенная аналогия:

```text
IP address — адрес дома.
Port — номер квартиры.
Socket — дверь/канал общения с конкретной квартирой.
```

Более технически:

```text
Socket — это программный интерфейс ОС,
через который процесс читает и пишет сетевые данные.
```

У процесса есть file descriptors. Socket descriptor — один из них.

Приложение не работает напрямую с сетевой картой. Оно работает через системные вызовы ОС:

```text
read / write / send / recv / accept / connect
```

Сильно упрощенная схема:

```text
Network
  ↓
Network card
  ↓
Kernel network stack
  ↓
Socket receive buffer
  ↓
Application reads data
```

Важно не говорить слишком буквально, что «сетевая карта кладет данные в буфер сокета». В реальности есть драйверы, ядро, DMA, network stack,
kernel buffers. Для урока можно сказать проще:

```text
Данные приходят из сети,
ядро ОС обрабатывает их
и делает доступными для чтения через socket.
```

---

## 18. Blocking socket read

В blocking-модели поток вызывает:

```java
socket.read(...)
```

Если данных нет, поток засыпает.

```text
Thread-1 → socket.read()
          ↓
          WAITING until data arrives
```

Если у нас 1000 соединений, и на каждом поток ждет данные:

```text
Socket-1    ← Thread-1 waits
Socket-2    ← Thread-2 waits
Socket-3    ← Thread-3 waits
...
Socket-1000 ← Thread-1000 waits
```

Это и есть модель:

```text
one connection/request → one waiting thread
```

Она простая, но при большом количестве соединений требует много потоков.

---

## 19. Non-blocking socket

В non-blocking-модели мы не хотим, чтобы поток засыпал на одном сокете.

Мы хотим:

```text
Если данных нет — не блокируй поток.
Сообщи мне, когда сокет будет готов к чтению/записи.
```

Проблема:

```text
А если сокетов 10 000?
Как понять, на каком появились данные?
```

Плохой вариант:

```text
while (true) {
  check socket-1
  check socket-2
  check socket-3
  ...
  check socket-10000
}
```

Это busy polling, он может сжечь CPU.

Для этого существуют механизмы I/O multiplexing.

В Java NIO мы работаем с `Selector`.

---

## 20. Selector

Selector — это механизм, который позволяет одному потоку следить за готовностью множества каналов.

Упрощенная аналогия:

```text
Есть 1000 дверей.
Вместо того чтобы поставить 1000 людей у дверей,
мы ставим одного вахтера у пульта с лампочками.

Лампочка загорелась — значит, конкретная дверь готова.
```

Технически:

```text
Selector — это Java NIO abstraction
над механизмами I/O multiplexing ОС.
```

Идея:

```text
Channel регистрируется в Selector.
Мы говорим: меня интересует OP_READ / OP_WRITE / ACCEPT / CONNECT.
Selector блокируется не на одном socket.read(),
а на ожидании событий по множеству зарегистрированных каналов.
```

Псевдокод:

```text
while (true) {
    selector.select();

    for (SelectionKey key : selector.selectedKeys()) {
        handle(key);
    }
}
```

Важно:

```text
Event loop thread тоже может "спать" внутри selector.select().
Но это не то же самое, что заблокировать отдельный поток на одном клиенте.

Он ждет события сразу по множеству соединений.
```

---

## 21. Event Loop

Event Loop — это поток, который в цикле:

1. ждет I/O-события;
2. получает готовые каналы;
3. обрабатывает сетевые события;
4. выполняет задачи из своей очереди;
5. снова возвращается к ожиданию событий.

Упрощенный цикл:

```text
while (running) {
    waitForIoEvents();
    processReadyChannels();
    runScheduledTasks();
    runTaskQueue();
}
```

Схема:

```text
EventLoop Thread
   ↓
selector.select()
   ↓
ready channels
   ↓
read bytes
   ↓
decode protocol
   ↓
invoke handlers
   ↓
write response if ready
   ↓
next loop iteration
```

Главное свойство:

```text
Один event loop thread может обслуживать много соединений,
если обработка каждого события короткая и не блокирующая.
```

---

## 22. Почему event loop нельзя блокировать

Если внутри event loop вызвать:

```java
Thread.sleep(1000);
```

или:

```java
blockingJdbcRepository.findById(id);
```

или:

```java
someMono.block();
```

то event loop перестает обслуживать остальные соединения.

Схема:

```text
EventLoop-1 handles Channel-1
  ↓
blocking call for 1 second
  ↓
Channel-2 waits
Channel-3 waits
Channel-4 waits
Channel-5 waits
```

Главный закон:

```text
Event loop должен быстро обрабатывать событие
и возвращаться к циклу.

Он не должен ждать БД, внешний HTTP-сервис,
Thread.sleep, файловую систему или тяжелый CPU-код.
```

Именно отсюда рождается одно из важнейших правил WebFlux:

```text
Не блокируй event loop.
```

---

## 23. EventLoop при входящих запросах

Представим: у нас один event loop thread и пять клиентов одновременно прислали запросы.

### 23.1. До запросов

```text
EventLoop-1
  ↓
selector.select()
  ↓
waiting for events
```

В selector зарегистрирован server socket на порту 8080.

### 23.2. Приходят соединения

Упрощенно:

```text
Client-1 ─┐
Client-2 ─┤
Client-3 ─┼──▶ Server port 8080
Client-4 ─┤
Client-5 ─┘
```

Ядро ОС принимает сетевые события, создает/обслуживает соединения, данные становятся доступны для чтения, selector видит готовые каналы.

### 23.3. Event loop просыпается

```text
selector.select() returns ready keys:
  Channel-1 ready
  Channel-2 ready
  Channel-3 ready
  Channel-4 ready
  Channel-5 ready
```

Event loop обрабатывает их по очереди:

```text
EventLoop-1:
  handle Channel-1
  handle Channel-2
  handle Channel-3
  handle Channel-4
  handle Channel-5
```

Главные выводы:

```text
Внутри одного event loop все выполняется последовательно.

Если Channel-1 заставит event loop ждать,
Channel-2..5 тоже будут ждать.

Экономия работает только если обработка событий короткая
и неблокирующая.
```

---

## 24. Реактивный алгоритм обращения во внешний сервис

Теперь представим WebFlux endpoint, который внутри вызывает внешний микросервис через reactive HTTP client.

Концептуально:

```text
1. EventLoop принимает входящий HTTP request.
2. Spring WebFlux вызывает controller.
3. Controller возвращает Mono.
4. Внутри Mono описан внешний HTTP-вызов.
5. Reactive client инициирует outbound request.
6. EventLoop не ждет ответ.
7. Он регистрирует интерес к событию на outbound channel.
8. Поток освобождается и обслуживает другие события.
9. Когда внешний сервис отвечает, ОС/selector/event loop получают событие.
10. Reactor pipeline продолжается.
11. Ответ записывается клиенту.
```

Схема:

```text
Incoming request
   ↓
Netty Channel A
   ↓
EventLoop receives bytes
   ↓
Spring WebFlux controller returns Mono
   ↓
WebClient sends request to external service via Channel B
   ↓
EventLoop is free
   ↓
External response arrives on Channel B
   ↓
Pipeline continues
   ↓
Response written to Channel A
```

Главная мысль:

```text
Мы не держим отдельный поток на время ожидания внешнего сервиса.
Мы держим описание продолжения работы.
```

---

## 25. Как это приводит к Netty, WebFlux и Reactor

Теперь можно связать все уровни:

```text
OS level
  ↓
non-blocking sockets / selector
  ↓
event loop
  ↓
Netty Channel / ChannelPipeline
  ↓
Reactor Netty HttpServer
  ↓
Spring WebFlux
  ↓
Project Reactor: Mono / Flux
```

Полная схема для первого дня:

```text
HTTP request
   ↓
Netty Channel
   ↓
EventLoop
   ↓
Reactor Netty
   ↓
Spring HttpHandler
   ↓
WebFlux handler/controller
   ↓
Mono/Flux pipeline
   ↓
async I/O operations
   ↓
response write
```

Но в первом уроке не нужно глубоко объяснять `ChannelPipeline`, `HttpHandler`, `DispatcherHandler`, `WebFilter`, `HandlerMapping`. Это
следующий день.

Здесь достаточно сказать:

```text
WebFlux использует Reactor и non-blocking runtime,
чтобы строить обработку HTTP-запросов как асинхронную цепочку сигналов,
а не как поток, который сидит и ждет каждый I/O-вызов.
```

---

## 26. Почему нельзя просто завернуть blocking-код в Mono

Плохой код:

```java

@GetMapping("/client/{id}")
public Mono<ClientDto> getClient(@PathVariable String id) {
    Client client = blockingRepository.findById(id);
    return Mono.just(ClientDto.from(client));
}
```

Почему плохо?

Потому что blocking-вызов уже произошел до создания `Mono`.

```text
EventLoop entered controller
  ↓
blockingRepository.findById(id) blocks event loop
  ↓
DB responds
  ↓
Mono.just(result)
```

`Mono.just(...)` здесь ничего не спасает.

Еще один плохой вариант:

```java

@GetMapping("/client/{id}")
public Mono<ClientDto> getClient(@PathVariable String id) {
    return Mono.just(blockingRepository.findById(id))
            .map(ClientDto::from);
}
```

Проблема та же:

```text
Аргумент Mono.just(...) вычисляется сразу.
blockingRepository.findById(id) блокирует текущий поток.
```

Если временно приходится использовать blocking API, нужно явно вынести его на отдельный scheduler:

```java

@GetMapping("/client/{id}")
public Mono<ClientDto> getClient(@PathVariable String id) {
    return Mono.fromCallable(() -> blockingRepository.findById(id))
            .subscribeOn(Schedulers.boundedElastic())
            .map(ClientDto::from);
}
```

Но важно проговорить:

```text
Это мост к blocking-миру, а не идеальный reactive-код.

Лучшее решение — использовать настоящий non-blocking драйвер/клиент,
например R2DBC вместо JDBC или WebClient вместо blocking HTTP client.
```

---

## 27. Где WebFlux полезен

WebFlux особенно полезен, когда система I/O-heavy:

- много одновременных запросов;
- много сетевых вызовов;
- есть reactive HTTP clients;
- есть reactive DB/cache drivers;
- нужны streaming responses;
- нужны long-lived connections;
- важна эффективность при high concurrency;
- нужно управлять backpressure.

Примеры:

```text
API Gateway
BFF with many downstream calls
streaming API
SSE endpoint
high-concurrency integration service
reactive data pipeline
```

---

## 28. Где WebFlux может быть лишним

WebFlux может быть не лучшим выбором, если:

- приложение простое CRUD;
- вся работа идет через blocking JDBC/JPA;
- команда не понимает reactive-модель;
- нет высокой concurrent I/O-нагрузки;
- основная нагрузка CPU-bound;
- код становится сложнее без реальной пользы;
- observability/debugging команды пока не готовы к реактивной модели.

Честная фраза:

```text
WebFlux — не замена Spring MVC во всех случаях.
Это инструмент для конкретной модели нагрузки и архитектуры.
```

---

## 29. Virtual Threads: где они в этой картине

Нельзя игнорировать virtual threads, потому что слушатели обязательно спросят:

```text
А зачем WebFlux, если есть virtual threads?
```

На первом уроке достаточно короткой рамки:

```text
Virtual threads решают часть проблемы thread-per-request иначе.

Они позволяют писать blocking-style код,
но делать сами Java-потоки намного легче,
чем platform threads.
```

Сравнение на уровне идеи:

```text
Spring MVC + platform threads:
  один request удерживает дорогой OS/platform thread

Spring MVC + virtual threads:
  один request выглядит как отдельный thread,
  но этот thread легковесный и не удерживает OS thread на весь lifetime

WebFlux + Netty:
  небольшое число event-loop threads обслуживает много соединений,
  а приложение описывает работу через non-blocking reactive pipeline
```

Важно:

```text
Virtual threads не отменяют WebFlux.
Они дают альтернативную модель конкурентности.
```

Глубокое сравнение лучше вынести в отдельный урок или appendix:

```text
WebFlux vs MVC + Virtual Threads
```

---

## 30. Live coding для первого урока

### 30.1. Blocking endpoint

```java

@RestController
@RequestMapping("/day1")
public class Day1Controller {

    @GetMapping("/blocking")
    public String blocking() throws InterruptedException {
        log("blocking: start");
        Thread.sleep(1000);
        log("blocking: end");
        return "blocking";
    }

    private void log(String message) {
        System.out.println(message + " | " + Thread.currentThread().getName());
    }
}
```

Объяснение:

```text
Thread.sleep имитирует blocking wait.
Поток не делает полезную работу,
но остается занятым этим request-ом.
```

### 30.2. Reactive delay endpoint

```java

@RestController
@RequestMapping("/day1")
public class Day1Controller {

    @GetMapping("/reactive-delay")
    public Mono<String> reactiveDelay() {
        log("reactive-delay: controller entered");

        return Mono.delay(Duration.ofSeconds(1))
                .doOnSubscribe(s -> log("reactive-delay: subscribed"))
                .doOnNext(v -> log("reactive-delay: delay emitted"))
                .thenReturn("reactive");
    }

    private void log(String message) {
        System.out.println(message + " | " + Thread.currentThread().getName());
    }
}
```

Объяснение:

```text
Mono.delay не блокирует поток ожиданием.
Он планирует сигнал на будущее.
Когда сигнал приходит, pipeline продолжается.
```

Важно:

```text
Это учебная демонстрация разницы между blocking wait и non-blocking wait.
Это не честный benchmark БД или внешнего сервиса.
```

### 30.3. Ошибка: blocking внутри Mono.just

```java

@GetMapping("/bad-mono-just")
public Mono<String> badMonoJust() {
    return Mono.just(blockingCall());
}

private String blockingCall() {
    sleep(1000);
    return "result";
}
```

Объяснение:

```text
blockingCall() выполняется сразу,
до создания Mono.
Mono.just здесь не делает код non-blocking.
```

### 30.4. Временный мост: fromCallable + boundedElastic

```java

@GetMapping("/legacy")
public Mono<String> legacy() {
    return Mono.fromCallable(this::blockingCall)
            .subscribeOn(Schedulers.boundedElastic());
}
```

Объяснение:

```text
Если мы вынуждены использовать blocking API,
мы не должны блокировать event loop.
Мы переносим blocking-работу на boundedElastic.
```

Но:

```text
Это компромисс.
Идеальный reactive pipeline должен использовать non-blocking clients/drivers.
```

---

## 31. Нагрузочная демонстрация

Можно использовать `hey`:

```bash
hey -n 1000 -c 100 http://localhost:8080/day1/blocking
```

```bash
hey -n 1000 -c 100 http://localhost:8080/day1/reactive-delay
```

Что нужно проговорить перед демонстрацией:

```text
Мы не доказываем, что WebFlux всегда быстрее.
Мы демонстрируем разницу в модели ожидания.

Thread.sleep удерживает поток.
Mono.delay планирует событие и освобождает поток.
```

Что можно смотреть:

- latency;
- throughput;
- thread names;
- количество активных потоков;
- поведение под concurrency;
- логи `doOnSubscribe`, `doOnNext`, `doFinally`.

---

## 32. Контрольные вопросы

После первого урока слушатель должен ответить:

1. Что запускает ОС при `java -jar app.jar`?
2. Почему JAR не является обычным native executable для ОС?
3. Чем процесс отличается от потока?
4. Почему процесс можно назвать единицей ресурсов?
5. Почему поток можно назвать единицей выполнения?
6. Что находится в общем адресном пространстве процесса?
7. Что индивидуально для каждого потока?
8. Что делает scheduler?
9. Чем `RUNNING` отличается от `RUNNABLE`?
10. Что значит, что поток находится в `WAITING`?
11. Что происходит с потоком во время blocking I/O?
12. Почему нельзя говорить, что CPU обязательно простаивает, когда поток ждет I/O?
13. Почему много waiting threads — проблема?
14. Чем async отличается от non-blocking?
15. Почему `CompletableFuture.supplyAsync` может быть async, но не non-blocking?
16. Что такое socket на уровне идеи?
17. Зачем нужен selector?
18. Что делает event loop?
19. Почему event loop нельзя блокировать?
20. Почему `Mono.just(blockingCall())` — плохой reactive-код?
21. Что такое backpressure?
22. Где WebFlux полезен?
23. Где WebFlux может быть лишним?
24. Как virtual threads связаны с этой темой?

---

## 33. Главные тезисы первого дня

```text
1. Reactive programming нужно начинать понимать не с операторов,
   а с модели потоков и I/O.

2. Blocking I/O удерживает request-thread во время ожидания.

3. CPU не обязан простаивать целиком,
   но waiting thread остается ресурсом.

4. Platform thread требует память под stack
   и участвует в scheduling/context switching.

5. Thread-per-request модель проста и хороша,
   но имеет предел при большом количестве concurrent I/O waits.

6. Async не равно non-blocking.
   Async может просто перенести blocking на другой поток.

7. Non-blocking I/O позволяет не ждать данные в отдельном потоке.

8. Selector позволяет одному потоку следить за множеством каналов.

9. Event loop эффективен только пока его не блокируют.

10. WebFlux/Reactor дают модель приложения поверх non-blocking runtime.

11. Mono/Flux — это не значения,
    а описание асинхронного потока сигналов.

12. Reactive Streams добавляют backpressure:
    consumer может контролировать demand.

13. WebFlux не всегда быстрее и не всегда нужен.

14. Если внутри WebFlux писать blocking-код,
    можно потерять смысл реактивной модели.
```

---

## 34. Что вынести в main-plan.md

В `main-plan.md`, который видят зрители, лучше оставить меньше текста и больше схем.

### Слайд/блок 1

```text
Reactive не ускоряет внешний мир.
Reactive экономит потоки во время ожидания I/O.
```

### Слайд/блок 2

```text
Blocking model:
Request → Thread → DB call → WAITING → Response
```

### Слайд/блок 3

```text
Process = resources
Thread = execution
```

### Слайд/блок 4

```text
RUNNING ↔ RUNNABLE ↔ WAITING
```

### Слайд/блок 5

```text
1000 blocking requests → many waiting threads
```

### Слайд/блок 6

```text
Async ≠ Non-blocking
```

### Слайд/блок 7

```text
Socket → Selector → EventLoop
```

### Слайд/блок 8

```text
Never block EventLoop
```

### Слайд/блок 9

```text
Reactive Streams:
Publisher → Subscriber
Subscriber → request(n)
```

### Слайд/блок 10

```text
WebFlux = non-blocking HTTP stack + Reactor pipeline
```

---

## 35. Что перенести в appendix

В основной рассказ лучше не перегружать этим:

- подробности instruction pointer;
- TCB;
- физические регистры CPU;
- CPU cache L1/L2/L3;
- page faults;
- ELF/PE segments;
- segmentation fault;
- metaspace/code cache/native memory глубоко;
- epoll/kqueue/io_uring глубоко;
- Netty ChannelPipeline глубоко;
- Reactor operator fusion;
- publishOn/subscribeOn;
- boundedElastic internals;
- R2DBC transactions.

Это хорошие темы, но не для первого захода. Их можно раскрывать позже, когда у слушателя уже есть главная модель.

---

## 36. Источники для автора

- Spring WebFlux reference: WebFlux uses Reactor as the reactive library of choice; Reactor provides `Mono` and `Flux` and supports Reactive
  Streams backpressure.
- Project Reactor reference: asynchronous non-blocking code allows execution to switch to another active task using the same resources and
  return later when async processing has completed.
- OpenJDK JEP 444 / Oracle docs: platform threads are wrappers over OS threads; virtual threads do not capture an OS thread for their whole
  lifetime.
- Reactor Netty reference: Reactor Netty uses Event Loop Group; default worker thread count is based on available processors with a minimum
  of 4.

---

# Финальная формула первого урока

```text
Reactive programming for backend =
  не "магический быстрый код",
  а способ строить I/O-heavy приложения так,
  чтобы не удерживать дорогие platform threads
  во время ожидания внешнего мира.
```

Переход ко второму уроку:

```text
Теперь, когда мы поняли проблему blocking I/O,
можно разобрать путь HTTP-запроса в Spring WebFlux:

Netty Channel
  ↓
EventLoop
  ↓
Reactor Netty HttpServer
  ↓
Spring HttpHandler
  ↓
WebFilter
  ↓
DispatcherHandler
  ↓
@RestController
  ↓
Mono/Flux response pipeline
```
