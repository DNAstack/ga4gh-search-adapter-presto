package com.dnastack.ga4gh.search.adapter.presto.exception;

import lombok.Getter;

public class TableApiErrorException extends RuntimeException {
    @Getter
    private final Exception previousException;

    @Getter
    private final Class<?> responseClass;

    public TableApiErrorException(Exception previousException, Class<?> responseClass) {
        this.previousException = previousException;
        this.responseClass = responseClass;
    }
}
