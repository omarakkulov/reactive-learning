package com.akkulov.reactive_learning.modules.V6_threads_schedulers_practice.lesson06;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Slf4j
@Configuration
public class Lesson06SchedulerConfiguration {

    public static final String CRYPTO_SCHEDULER_BEAN = "lesson06CryptoScheduler";

    @Bean(name = CRYPTO_SCHEDULER_BEAN, destroyMethod = "dispose")
    public Scheduler lesson06CryptoScheduler() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int parallelism = Math.max(1, availableProcessors / 2);

        log.info(
                "[LESSON-06] создаём dedicated CPU Scheduler: name=lesson06-crypto, parallelism={}, availableProcessors={}",
                parallelism,
                availableProcessors
        );

        return Schedulers.newParallel("lesson06-crypto", parallelism);
    }
}
