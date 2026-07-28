package com.akkulov.reactive_learning.modules.V5_reactive_streams_practice.lesson05;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping("/api/lesson-05")
public class ReactiveStreamsPracticeController {

	private final AtomicInteger coldSubscriptionCounter = new AtomicInteger();

	@GetMapping("/mono-simple")
	public Mono<Map<String, String>> monoSimple() {
		logStage("mono-simple", "controller method invoked");

		return Mono.just(Map.of("value", "Hello from Mono"))
				.doOnSubscribe(subscription -> logStage("mono-simple", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("mono-simple", "doOnRequest n=" + requested))
				.doOnNext(value -> logStage("mono-simple", "doOnNext value=" + value))
				.doOnSuccess(value -> logStage("mono-simple", "doOnSuccess value=" + value))
				.doFinally(signalType -> logStage("mono-simple", "doFinally signal=" + signalType));
	}

	@GetMapping("/lazy")
	public Mono<Map<String, String>> lazy() {
		logStage("lazy", "controller method invoked");

		Mono<Map<String, String>> pipeline = Mono.fromSupplier(() -> {
					logStage("lazy", "supplier executed");
					return Map.of("value", "Lazy value");
				})
				.doOnSubscribe(subscription -> logStage("lazy", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("lazy", "doOnRequest n=" + requested))
				.doOnNext(value -> logStage("lazy", "doOnNext value=" + value))
				.doOnSuccess(value -> logStage("lazy", "doOnSuccess value=" + value))
				.doFinally(signalType -> logStage("lazy", "doFinally signal=" + signalType));

		logStage("lazy", "pipeline assembled, supplier has not executed yet");
		return pipeline;
	}

	@GetMapping("/delay")
	public Mono<Map<String, String>> delay() {
		logStage("delay", "controller method invoked");

		return Mono.delay(Duration.ofSeconds(1))
				.doOnSubscribe(subscription -> logStage("delay", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("delay", "doOnRequest n=" + requested))
				.doOnNext(value -> logStage("delay", "doOnNext timerValue=" + value))
				.map(value -> Map.of("value", "Delayed response"))
				.doOnSuccess(value -> logStage("delay", "doOnSuccess value=" + value))
				.doFinally(signalType -> logStage("delay", "doFinally signal=" + signalType));
	}

	@GetMapping(value = "/flux", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<LessonEvent> flux() {
		logStage("flux", "controller method invoked");

		return Flux.range(1, 10)
				.delayElements(Duration.ofMillis(300))
				.map(index -> new LessonEvent(index, "event-" + index))
				.doOnSubscribe(subscription -> logStage("flux", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("flux", "doOnRequest n=" + requested))
				.doOnNext(value -> logStage("flux", "doOnNext value=" + value))
				.doOnComplete(() -> logStage("flux", "doOnComplete"))
				.doFinally(signalType -> logStage("flux", "doFinally signal=" + signalType));
	}

	@GetMapping("/empty")
	public Mono<Map<String, String>> empty() {
		logStage("empty", "controller method invoked");

		return Mono.<Map<String, String>>empty()
				.doOnSubscribe(subscription -> logStage("empty", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("empty", "doOnRequest n=" + requested))
				.doOnSuccess(value -> logStage("empty", "doOnSuccess value=" + value))
				.doFinally(signalType -> logStage("empty", "doFinally signal=" + signalType));
	}

	@GetMapping("/error")
	public Mono<Map<String, String>> error() {
		logStage("error", "controller method invoked");

		return Mono.<Map<String, String>>error(new IllegalStateException("Lesson 05 demo error"))
				.doOnSubscribe(subscription -> logStage("error", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("error", "doOnRequest n=" + requested))
				.doOnError(error -> logStage("error", "doOnError error=" + error.getMessage()))
				.doFinally(signalType -> logStage("error", "doFinally signal=" + signalType));
	}

	@GetMapping(value = "/cold", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
	public Flux<LessonEvent> cold() {
		logStage("cold", "controller method invoked");

		return Flux.defer(() -> {
					int currentSubscription = coldSubscriptionCounter.incrementAndGet();
					logStage("cold", "new cold sequence for subscription=" + currentSubscription);
					return Flux.range(1, 3)
							.delayElements(Duration.ofMillis(250))
							.map(index -> new LessonEvent(index, "subscription-" + currentSubscription + "-value-" + index));
				})
				.doOnSubscribe(subscription -> logStage("cold", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("cold", "doOnRequest n=" + requested))
				.doOnNext(value -> logStage("cold", "doOnNext value=" + value))
				.doOnComplete(() -> logStage("cold", "doOnComplete"))
				.doFinally(signalType -> logStage("cold", "doFinally signal=" + signalType));
	}

	@GetMapping("/hot-demo-explanation")
	public Mono<Map<String, String>> hotDemoExplanation() {
		logStage("hot-demo-explanation", "controller method invoked");

		return Mono.just(Map.of(
				"concept", "Hot Publisher",
				"analogy", "Radio broadcast: if you join later, you do not hear what already happened",
				"note", "Deep practice with Sinks will be covered later"
		))
				.doOnSubscribe(subscription -> logStage("hot-demo-explanation", "doOnSubscribe"))
				.doOnRequest(requested -> logStage("hot-demo-explanation", "doOnRequest n=" + requested))
				.doOnNext(value -> logStage("hot-demo-explanation", "doOnNext value=" + value))
				.doFinally(signalType -> logStage("hot-demo-explanation", "doFinally signal=" + signalType));
	}

	private void logStage(String endpoint, String stage) {
		log.info("[lesson-05:{}] {} | thread={}", endpoint, stage, Thread.currentThread().getName());
	}
}
