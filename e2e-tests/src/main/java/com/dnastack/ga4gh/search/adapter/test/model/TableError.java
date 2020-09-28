package com.dnastack.ga4gh.search.adapter.test.model;

import lombok.Data;

import java.util.Map;

@Data
public class TableError {

    private String source;
    private String message;
    private ErrorCode code;
    private Map<String, Object> attributes;

    public enum ErrorCode {
        AUTH_CHALLENGE, // Non-standard
        PRESTO_QUERY, // Non-standard
        ERROR_RESPONSE,
        NO_RESPONSE
    }
}
