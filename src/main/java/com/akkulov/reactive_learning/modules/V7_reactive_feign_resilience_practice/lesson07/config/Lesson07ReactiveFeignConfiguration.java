package com.akkulov.reactive_learning.modules.V7_reactive_feign_resilience_practice.lesson07.config;

import com.akkulov.reactive_learning.modules.V7_reactive_feign_resilience_practice.lesson07.http.Lesson07ProductReactiveClient;
import io.netty.resolver.DefaultAddressResolverGroup;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactivefeign.spring.config.EnableReactiveFeignClients;
import reactivefeign.webclient.WebClientFeignCustomizer;
import reactor.netty.http.client.HttpClient;

@Configuration(proxyBeanMethods = false)
@EnableReactiveFeignClients(clients = Lesson07ProductReactiveClient.class)
public class Lesson07ReactiveFeignConfiguration {

    @Bean
    WebClient.Builder lesson07WebClientBuilder() {
        return WebClient.builder();
    }

    @Bean
    WebClientFeignCustomizer lesson07WebClientFeignCustomizer() {
        HttpClient httpClient = HttpClient.create()
                .resolver(DefaultAddressResolverGroup.INSTANCE);

        return webClientBuilder -> webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(httpClient));
    }
}
