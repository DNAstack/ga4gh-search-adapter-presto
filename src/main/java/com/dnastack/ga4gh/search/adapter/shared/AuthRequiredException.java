package com.dnastack.ga4gh.search.adapter.shared;

import lombok.Getter;

import java.io.IOException;

@Getter
public class AuthRequiredException extends IOException {
    private final SearchAuthRequest authorizationRequest;

    public AuthRequiredException(SearchAuthRequest authorizationRequest) {
        this.authorizationRequest = authorizationRequest;
    }
}
