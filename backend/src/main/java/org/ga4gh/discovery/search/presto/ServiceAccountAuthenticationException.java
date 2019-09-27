package org.ga4gh.discovery.search.presto;

public class ServiceAccountAuthenticationException extends RuntimeException {

    public ServiceAccountAuthenticationException(String message) {
        super(message);
    }

    public ServiceAccountAuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }
}
