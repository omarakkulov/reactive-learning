package com.akkulov.reactive_learning.modules.V4_webflux_runtime_practice.lesson04;

import java.time.Instant;

import com.akkulov.reactive_learning.modules.V4_webflux_runtime_practice.lesson04.model.Lesson04ProfileResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/api/lesson-04")
public class RuntimeRequestPathController {

    @GetMapping(value = "/mono-object", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<Lesson04ProfileResponse> monoObject(@RequestParam(defaultValue = "student") String name) {
        log.info(
                "[MONO] controller: метод вызван, name={} | thread={}",
                name,
                Thread.currentThread().getName()
        );

        Mono<Lesson04ProfileResponse> responsePublisher = Mono.fromSupplier(() -> {
                    log.info(
                            "[MONO] supplier: создаём Lesson04ProfileResponse после subscription и demand | thread={}",
                            Thread.currentThread().getName()
                    );
                    return new Lesson04ProfileResponse(
                            name,
                            "Hello, " + name,
                            Instant.now().toString()
                    );
                })
                // `doOnSubscribe` не является еще одной нашей терминальной командой. Это оператор-наблюдатель. Он сообщает, что кто-то подписался на этот
                //участок цепочки.
                .doOnSubscribe(subscription -> log.info(
                        "[MONO] doOnSubscribe: subscription дошла до Mono | thread={}",
                        Thread.currentThread().getName()
                ))
                //В нашей лекции doOnRequest нужен, чтобы показать промежуточный шаг между подпиской и выполнением источника:
                .doOnRequest(requested -> log.info(
                        "[MONO] doOnRequest: request(n)={} | thread={}",
                        requested,
                        Thread.currentThread().getName()
                ))
                .doOnNext(response -> log.info(
                        "[MONO] doOnNext: response={} | thread={}",
                        response,
                        Thread.currentThread().getName()
                ))
                // оставлен как диагностический hook на случай непредвиденной ошибки
                .doOnError(error -> log.info(
                        "[MONO] doOnError: type={}, message={} | thread={}",
                        error.getClass().getSimpleName(),
                        error.getMessage(),
                        Thread.currentThread().getName()
                ))
                .doFinally(signalType -> log.info(
                        "[MONO] doFinally: signal={} | thread={}",
                        signalType,
                        Thread.currentThread().getName()
                ));

        log.info(
                "[MONO] return: возвращаем Mono без subscribe() | thread={}",
                Thread.currentThread().getName()
        );

        return responsePublisher;
    }

}
