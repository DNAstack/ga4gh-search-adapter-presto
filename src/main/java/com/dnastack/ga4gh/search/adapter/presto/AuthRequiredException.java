package com.dnastack.ga4gh.search.adapter.presto;

import lombok.Getter;

import java.io.IOException;
import java.util.Map;

@Getter
public class AuthRequiredException extends IOException {
    private final Map<String, Object> credentialsRequest;

    public AuthRequiredException(Map<String, Object> credentialsRequest) {
        this.credentialsRequest = credentialsRequest;
    }
}
