package com.dnastack.ga4gh.search.adapter.shared;

import com.dnastack.ga4gh.search.tables.TableError;
import com.dnastack.ga4gh.search.tables.TableError.ErrorCode;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NonNull;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalControllerExceptionHandler {


    @ExceptionHandler(CapturedSearchException.class)
    public ResponseEntity<?> handleCapturedSearchException(CapturedSearchException e) {
        List<TableError> errors = ErrorUtils.handleErrors(e);
        HttpHeaders headers = new HttpHeaders();
        int statusCode = 500;
        for (TableError error : errors) {
            if (error.getCode().equals(ErrorCode.AUTH_CHALLENGE)) {
                headers.add("WWW-authenticate", "GA4GH-Search realm=\"" + escapeQuotes(error.getSource()) + "\"");
                if (statusCode == 500) {
                    statusCode = 401;
                }
            } else if (error.getCode().equals(ErrorCode.NO_RESPONSE)) {
                if (statusCode == 500) {
                    statusCode = 504;
                }
            }
        }
        return ResponseEntity.status(statusCode)
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .headers(headers)
            .body(new ErrorResponse(errors));
    }

    private static String escapeQuotes(String s) {
        return s.replace("\"", "\\\"");
    }

    @Data
    private static class ErrorResponse {

        @NonNull
        private List<TableError> errors;
    }
}
