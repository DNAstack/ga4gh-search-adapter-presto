package com.dnastack.ga4gh.search.tables;

import java.util.Map;
import lombok.Data;

@Data
public class TableError {

    private String source;
    private String message;
    private ErrorCode code;
    private Map<String,String> attributes;

    public enum ErrorCode {
        AUTH_CHALLENGE,TIMEOUT
    }
}
