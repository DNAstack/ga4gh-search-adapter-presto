package com.dnastack.ga4gh.search.adapter.shared;

import lombok.Data;

@Data
public class CapturedSearchException extends RuntimeException {

    private String source;
    private String statement;
    private long elapsedTime;

    public CapturedSearchException(String source, String statement, String message, Long elapsedTime, Throwable cause) {
        super(message, cause);
        this.source = source;
        this.statement = statement;
        this.elapsedTime = elapsedTime == null ? 0 : elapsedTime;
    }
}
