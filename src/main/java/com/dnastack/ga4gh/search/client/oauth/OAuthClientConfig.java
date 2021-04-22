package com.dnastack.ga4gh.search.client.oauth;

import com.dnastack.ga4gh.search.client.common.SimpleLogger;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import feign.Feign;
import feign.Logger;
import feign.codec.Encoder;
import feign.form.FormEncoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.okhttp.OkHttpClient;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

@Slf4j
@Data
@Configuration
public class OAuthClientConfig {
    /**
     * The URI to use for authenticated service to service communication. This is expected to be an OIDC compliant token
     * endpoint which will accept {@code client_credentials}
     */
    String authenticationUri;

    /**
     * The service account client id which will be used to authenticate this service against others
     */
    String clientId;

    /**
     * The service account client secret which will be used to authenticate this service against others
     */
    String clientSecret;

    String audience;

    /*
     * Requested resource
     */
    String resource;

    @Autowired
    SimpleLogger simpleLogger;

    Encoder encoder;

    private ObjectMapper mapper = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    @Bean
    public OAuthClient oAuthClient() {
        return Optional
            .ofNullable(authenticationUri)
            .flatMap(authenticationUri -> Optional.of(
                Feign.builder()
                    .client(new OkHttpClient())
                    .encoder(new FormEncoder(new JacksonEncoder(mapper)))
                    .decoder(new JacksonDecoder(mapper))
                    .logger(simpleLogger)
                    .logLevel(Logger.Level.BASIC)
                    .target(OAuthClient.class, authenticationUri)
                )
            )
            .orElse(client -> {
                log.warn("The client for token exchange is not defined.");
                return null;
            });
    }
}
