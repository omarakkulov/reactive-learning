# Эксперименты: Blocking vs Non-Blocking

Перед экспериментами запусти приложение:

```bash
./gradlew bootRun
```

В другом терминале выполняй запросы через `curl`.

## 1. Посмотреть текущий поток

Команда:

```bash
curl "http://localhost:8080/api/lesson-01/thread"
```

Что увидеть:

```json
{"threadName":"reactor-http-nio-..."}
```

Название может отличаться, но при запуске на Reactor Netty обычно будет видно `reactor-http-nio-*`.

Вывод:

```text
HTTP-запрос обрабатывается не servlet worker thread, а Netty/Reactor event loop потоком.
```

## 2. Blocking endpoint

Команда:

```bash
curl "http://localhost:8080/api/lesson-01/blocking-sleep?delayMs=1000"
```

Что увидеть:

```json
{
  "mode": "blocking",
  "requestedDelayMs": 1000,
  "elapsedMs": 1000,
  "startedOnThread": "reactor-http-nio-...",
  "completedOnThread": "reactor-http-nio-..."
}
```

Вывод:

```text
На время Thread.sleep текущий поток реально заблокирован.
```

Важный комментарий для занятия: это плохой код для WebFlux. Мы пишем его специально, чтобы увидеть
проблему руками.

## 3. Non-blocking endpoint

Команда:

```bash
curl "http://localhost:8080/api/lesson-01/non-blocking-delay?delayMs=1000"
```

Что увидеть:

```json
{
  "mode": "non-blocking",
  "requestedDelayMs": 1000,
  "elapsedMs": 1000,
  "startedOnThread": "reactor-http-nio-...",
  "completedOnThread": "parallel-..."
}
```

Имя `completedOnThread` может отличаться. Главное: текущий поток не засыпал через `Thread.sleep`.
Продолжение pipeline произошло после сигнала от Reactor.

Вывод:

```text
Запрос все равно завершился примерно через секунду, но поток не обязан был стоять и ждать эту секунду.
```

## 4. Несколько параллельных blocking-запросов

Команда без внешних утилит:

```bash
for i in {1..20}; do
  curl -s "http://localhost:8080/api/lesson-01/blocking-sleep?delayMs=1000" > /dev/null &
done
wait
```

Что смотреть:

- время выполнения всей пачки;
- логи приложения;
- имена потоков;
- задержки ответа, если увеличить количество запросов.

Вывод:

```text
Blocking-вызов занимает event loop поток. При росте конкуренции это мешает обслуживать другие события.
```

## 5. Несколько параллельных non-blocking-запросов

Команда:

```bash
for i in {1..20}; do
  curl -s "http://localhost:8080/api/lesson-01/non-blocking-delay?delayMs=1000" > /dev/null &
done
wait
```

Что смотреть:

- вся пачка должна завершиться примерно около заданной задержки, а не строго последовательно;
- event loop не должен быть занят `Thread.sleep`;
- в логах видно планирование и завершение delayed-сигналов.

Вывод:

```text
Non-blocking pipeline лучше подходит для большого количества одновременного ожидания.
```

## 6. Если установлен hey или wrk

Это необязательный эксперимент.

Пример с `hey`:

```bash
hey -n 100 -c 20 "http://localhost:8080/api/lesson-01/blocking-sleep?delayMs=1000"
hey -n 100 -c 20 "http://localhost:8080/api/lesson-01/non-blocking-delay?delayMs=1000"
```

Здесь важно не превращать занятие в benchmark. Цель не доказать абсолютные цифры, а увидеть модель:

```text
blocking занимает поток ожиданием;
non-blocking возвращает поток в обработку других событий.
```

## 7. Контрольный вопрос после экспериментов

Почему `non-blocking-delay` не отвечает мгновенно, если он non-blocking?

Ожидаемый ответ:

```text
Потому что non-blocking не означает "без latency". Ожидание остается. Меняется то, что делает поток
во время ожидания.
```
