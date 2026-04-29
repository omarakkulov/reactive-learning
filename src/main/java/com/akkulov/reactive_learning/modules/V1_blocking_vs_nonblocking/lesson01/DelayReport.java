package com.akkulov.reactive_learning.modules.V1_blocking_vs_nonblocking.lesson01;

public record DelayReport(
		String mode,
		long requestedDelayMs,
		long elapsedMs,
		String startedOnThread,
		String completedOnThread
) {
}
