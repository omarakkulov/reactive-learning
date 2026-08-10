package com.akkulov.reactive_learning.modules.V6_threads_schedulers_practice.lesson06;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

/**
 * Учебная CPU-bound работа.
 *
 * <p>Это не production-алгоритм хранения паролей. Повторный SHA-256 нужен только для того,
 * чтобы Thread действительно вычислял, а не ждал через {@code Thread.sleep(...)}.</p>
 */
@Service
public class CpuIntensiveCryptoService {

    public CryptoComputation repeatedlyHashFor(String payload, long durationMs) {
        MessageDigest digest = sha256();
        byte[] current = payload.getBytes(StandardCharsets.UTF_8);
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationMs);
        long iterations = 0;

        do {
            current = digest.digest(current);
            iterations++;
        } while (System.nanoTime() < deadlineNanos);

        return new CryptoComputation(HexFormat.of().formatHex(current), iterations);
    }

    private MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 должен поддерживаться JVM", error);
        }
    }

    public record CryptoComputation(String hash, long iterations) {
    }
}
