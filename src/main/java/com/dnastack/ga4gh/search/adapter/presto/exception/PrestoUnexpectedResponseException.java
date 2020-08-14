package com.dnastack.ga4gh.search.adapter.presto.exception;

import lombok.Getter;

import java.io.IOException;

@Getter
public class PrestoUnexpectedResponseException extends RuntimeException {
    private final int code;
    private final String logMessage;

    public PrestoUnexpectedResponseException(int code, String message, String logMessage) {
        super(message);
        this.code = code;
        this.logMessage = logMessage;
    }

    public PrestoUnexpectedResponseException(int code, String message, String logMessage, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.logMessage = logMessage;
    }
}
