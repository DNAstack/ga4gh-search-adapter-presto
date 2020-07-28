package com.dnastack.ga4gh.search.adapter.presto;

import lombok.Getter;

import java.io.IOException;

@Getter
public class PrestoQueryFailedException extends RuntimeException {
    private final int code;

    public PrestoQueryFailedException(int code, String message) {
        super(message);
        this.code = code;
    }
}
