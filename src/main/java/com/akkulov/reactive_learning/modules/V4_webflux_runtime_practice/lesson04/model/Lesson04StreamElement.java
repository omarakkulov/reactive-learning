package com.akkulov.reactive_learning.modules.V4_webflux_runtime_practice.lesson04.model;

public record Lesson04StreamElement(
		String lesson,
		String scenario,
		int index,
		String value,
		String emittedAt
) {
}
