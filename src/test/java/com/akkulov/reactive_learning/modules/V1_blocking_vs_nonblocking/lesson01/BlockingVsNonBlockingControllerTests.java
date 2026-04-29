package com.akkulov.reactive_learning.modules.V1_blocking_vs_nonblocking.lesson01;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.test.web.reactive.server.WebTestClient;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@SpringBootTest
@AutoConfigureWebTestClient
class BlockingVsNonBlockingControllerTests {

	@Autowired
	private WebTestClient webTestClient;

	@Test
	void currentThreadReturnsThreadName() {
		webTestClient.get()
				.uri("/api/lesson-01/thread")
				.exchange()
				.expectStatus().isOk()
				.expectBody()
				.jsonPath("$.threadName").isNotEmpty();
	}

	@Test
	void blockingSleepReturnsDelayReport() {
		webTestClient.get()
				.uri("/api/lesson-01/blocking-sleep?delayMs=10")
				.exchange()
				.expectStatus().isOk()
				.expectBody()
				.jsonPath("$.mode").isEqualTo("blocking")
				.jsonPath("$.requestedDelayMs").isEqualTo(10)
				.jsonPath("$.startedOnThread").isNotEmpty()
				.jsonPath("$.completedOnThread").isNotEmpty();
	}

	@Test
	void nonBlockingDelayReturnsDelayReport() {
		webTestClient.get()
				.uri("/api/lesson-01/non-blocking-delay?delayMs=10")
				.exchange()
				.expectStatus().isOk()
				.expectBody()
				.jsonPath("$.mode").isEqualTo("non-blocking")
				.jsonPath("$.requestedDelayMs").isEqualTo(10)
				.jsonPath("$.startedOnThread").isNotEmpty()
				.jsonPath("$.completedOnThread").isNotEmpty();
	}

	@Test
	void reactorDelayEmitsAfterConfiguredDuration() {
		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofSeconds(5)))
				.expectSubscription()
				.expectNoEvent(Duration.ofSeconds(5))
				.expectNext(0L)
				.verifyComplete();
	}
}
