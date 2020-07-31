package com.dnastack.ga4gh.search.adapter.shared;

import com.dnastack.ga4gh.search.adapter.presto.PrestoNoSuchCatalogException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class GlobalControllerExceptionHandler {
    @ExceptionHandler(AuthRequiredException.class)
    public ResponseEntity<?> handleAuthRequiredException(AuthRequiredException e) {
        SearchAuthRequest cr = e.getAuthorizationRequest();
        return ResponseEntity.status(401)
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .header("WWW-Authenticate", "GA4GH-Search realm=\"" + escapeQuotes(cr.getKey()) + "\"")
                .body(Map.of("authorization-request", cr));
    }

    @ExceptionHandler(PrestoNoSuchCatalogException.class)
    public ResponseEntity<?> handleNoSuchCatalogException(PrestoNoSuchCatalogException ex){
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No such catalog: "+ex.getCatalog());
    }

    private static String escapeQuotes(String s) {
        return s.replace("\"", "\\\"");
    }

}
