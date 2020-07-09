package com.dnastack.ga4gh.search.adapter.test.model;

import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class ErrorResponse {

    private List<Error> errors;


    @Data
    public static class Error {

        private String source;
        private String message;
        private ErrorCode code;
        private Map<String, String> attributes;

        public enum ErrorCode {
            AUTH_CHALLENGE, TIMEOUT
        }

    }
}
