package com.akkulov.reactive_learning.modules.V6_threads_schedulers_practice.lesson06;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class SchedulerLessonControllerTests {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void currentThreadHasNoSchedulerBoundary() {
        Lesson06ExecutionResponse response = get("/api/lesson-06/current-thread");

        assertEquals("current-thread", response.scenario());
        assertEquals(response.controllerThread(), response.workThread());
        assertEquals("no-scheduler-boundary", response.result());
    }

    @Test
    void cpuWorkWithoutBoundaryStaysOnControllerThread() {
        Lesson06ExecutionResponse response = get(
                "/api/lesson-06/cpu-on-event-loop?payload=test&durationMs=2"
        );

        assertEquals("cpu-on-event-loop", response.scenario());
        assertEquals(response.controllerThread(), response.workThread());
        assertTrue(response.result().startsWith("sha256="));
        assertTrue(response.actualDurationMs() >= 1);
    }

    @Test
    void cpuWorkAfterPublishOnUsesParallelScheduler() {
        Lesson06ExecutionResponse response = get(
                "/api/lesson-06/cpu-on-parallel?payload=test&durationMs=2"
        );

        assertEquals("cpu-on-parallel", response.scenario());
        assertNotEquals(response.controllerThread(), response.workThread());
        assertTrue(response.workThread().startsWith("parallel-"));
        assertTrue(response.result().startsWith("sha256="));
    }

    @Test
    void blockingCallWithoutBoundaryStaysOnControllerThread() {
        Lesson06ExecutionResponse response = get(
                "/api/lesson-06/blocking-on-event-loop?userId=42&durationMs=2"
        );

        assertEquals("blocking-on-event-loop", response.scenario());
        assertEquals(response.controllerThread(), response.workThread());
        assertEquals("legacy-profile-for-42", response.result());
    }

    @Test
    void blockingCallWithSubscribeOnUsesBoundedElastic() {
        Lesson06ExecutionResponse response = get(
                "/api/lesson-06/blocking-on-bounded-elastic?userId=42&durationMs=2"
        );

        assertEquals("blocking-on-bounded-elastic", response.scenario());
        assertNotEquals(response.controllerThread(), response.workThread());
        assertTrue(response.workThread().contains("boundedElastic"));
        assertEquals("legacy-profile-for-42", response.result());
    }

    @Test
    void combinedPipelineUsesBoundedElasticThenParallel() {
        Lesson06CombinedExecutionResponse response = webTestClient.get()
                .uri("/api/lesson-06/blocking-then-cpu?userId=42&blockingDurationMs=2&cpuDurationMs=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Lesson06CombinedExecutionResponse.class)
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
        assertEquals("blocking-then-cpu", response.scenario());
        assertNotEquals(response.controllerThread(), response.blockingThread());
        assertTrue(response.blockingThread().contains("boundedElastic"));
        assertNotEquals(response.blockingThread(), response.cpuThread());
        assertTrue(response.cpuThread().startsWith("parallel-"));
        assertTrue(response.result().contains("legacy-profile-for-42"));
        assertTrue(response.result().contains("sha256="));
    }

    @Test
    void reverseCombinedPipelineUsesParallelThenBoundedElastic() {
        Lesson06CombinedExecutionResponse response = webTestClient.get()
                .uri("/api/lesson-06/cpu-then-blocking?payload=test&userId=42&cpuDurationMs=2&blockingDurationMs=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody(Lesson06CombinedExecutionResponse.class)
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
        assertEquals("cpu-then-blocking", response.scenario());
        assertNotEquals(response.controllerThread(), response.cpuThread());
        assertTrue(response.cpuThread().startsWith("parallel-"));
        assertNotEquals(response.cpuThread(), response.blockingThread());
        assertTrue(response.blockingThread().contains("boundedElastic"));
        assertTrue(response.result().contains("sha256="));
        assertTrue(response.result().contains("legacy-profile-for-42"));
    }

    @Test
    void rejectsDurationBelowAndAboveEducationalRange() {
        webTestClient.get()
                .uri("/api/lesson-06/cpu-on-event-loop?durationMs=0")
                .exchange()
                .expectStatus().isBadRequest();

        webTestClient.get()
                .uri("/api/lesson-06/blocking-on-bounded-elastic?durationMs=5001")
                .exchange()
                .expectStatus().isBadRequest();
    }

    private Lesson06ExecutionResponse get(String uri) {
        Lesson06ExecutionResponse response = webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus().isOk()
                .expectBody(Lesson06ExecutionResponse.class)
                .returnResult()
                .getResponseBody();

        assertNotNull(response);
        assertNotNull(response.controllerThread());
        assertNotNull(response.workThread());
        return response;
    }
}
