package com.dnastack.ga4gh.search.client.indexingservice;

import com.dnastack.ga4gh.search.client.common.SimpleLogger;
import com.dnastack.ga4gh.search.client.oauth.OAuthClientConfig;
import com.dnastack.ga4gh.search.client.oauth.model.AccessToken;
import com.dnastack.ga4gh.search.client.oauth.model.OAuthRequest;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import feign.Feign;
import feign.Logger;
import feign.RequestInterceptor;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.okhttp.OkHttpClient;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Getter
@Setter
@Configuration
@ConfigurationProperties("app.indexing-service")
public class IndexingServiceClientConfig {

    private String url;

    @Autowired
    private OAuthClientConfig oAuthClient;

    @Autowired
    private SimpleLogger simpleLogger;


    private ObjectMapper mapper = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private OAuthRequest getOAuthRequest() {
        OAuthRequest oAuthRequest = new OAuthRequest();
        oAuthRequest.setClientId(oAuthClient.getClientId());
        oAuthRequest.setClientSecret(oAuthClient.getClientSecret());
        oAuthRequest.setGrantType("client_credentials");
        oAuthRequest.setAudience(oAuthClient.getAudience());
        return oAuthRequest;
    }

    private RequestInterceptor getRequestInterceptor() {
        return (template) -> {
            AccessToken accessToken = oAuthClient.oAuthClient().getToken(getOAuthRequest());
            template.header("Authorization", "Bearer " + accessToken.getToken());
        };
    }

    @Bean
    public IndexingServiceClient indexingServiceClient() {
        // TODO Refactor with OAuthClientFactory
        if (url == null || oAuthClient == null) {
            log.warn("The client for Indexing Service is not defined.");
            return null;
        }
        return Feign.builder()
                    .client(new OkHttpClient())
                    .encoder(new JacksonEncoder(mapper))
                    .decoder(new JacksonDecoder(mapper))
                    .logger(simpleLogger)
                    .logLevel(Logger.Level.BASIC)
                    .requestInterceptor(getRequestInterceptor())
                    .target(IndexingServiceClient.class, url);

    }
}
