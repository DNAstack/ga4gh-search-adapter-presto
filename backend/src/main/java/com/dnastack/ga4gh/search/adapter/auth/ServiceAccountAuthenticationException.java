package com.dnastack.ga4gh.search.adapter.auth;

public class ServiceAccountAuthenticationException extends RuntimeException {

    public ServiceAccountAuthenticationException(String message) {
        super(message);
    }

    public ServiceAccountAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }
}
