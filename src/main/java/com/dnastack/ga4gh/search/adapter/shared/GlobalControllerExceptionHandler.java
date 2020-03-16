package com.dnastack.ga4gh.search.adapter.shared;

import com.dnastack.ga4gh.search.adapter.presto.AuthRequiredException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalControllerExceptionHandler {
    @ExceptionHandler(AuthRequiredException.class)
    public ResponseEntity<?> handleAuthRequiredException(AuthRequiredException e) {
        return ResponseEntity.status(401)
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .body(e.getCredentialsRequest());
    }
}
