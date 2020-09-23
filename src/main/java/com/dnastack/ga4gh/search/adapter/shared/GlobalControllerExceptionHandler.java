package com.dnastack.ga4gh.search.adapter.shared;

import brave.Tracer;
import com.dnastack.ga4gh.search.adapter.presto.exception.InvalidQueryJobException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoBadlyQualifiedNameException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoErrorException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoIOException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoInsufficientResourcesException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoInternalErrorException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoInvalidQueryException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchCatalogException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchColumnException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchSchemaException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoNoSuchTableException;
import com.dnastack.ga4gh.search.adapter.presto.exception.PrestoUnexpectedHttpResponseException;
import com.dnastack.ga4gh.search.adapter.presto.exception.QueryParsingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.UncheckedIOException;
import java.util.Map;

@RestControllerAdvice
@Slf4j
public class GlobalControllerExceptionHandler {
    @Autowired
    private Tracer tracer;


    @ExceptionHandler(AuthRequiredException.class)
    public ResponseEntity<?> handleAuthRequiredException(AuthRequiredException e) {
        SearchAuthRequest cr = e.getAuthorizationRequest();
        return ResponseEntity.status(401)
                .contentType(MediaType.APPLICATION_JSON_UTF8)
                .header("WWW-Authenticate", "GA4GH-Search realm=\"" + escapeQuotes(cr.getKey()) + "\"")
                .body(Map.of("authorization-request", cr, "trace_id", tracer.currentSpan().context().traceIdString()));
    }

    @ExceptionHandler({PrestoNoSuchCatalogException.class, PrestoNoSuchSchemaException.class, PrestoNoSuchTableException.class})
    public ResponseEntity<?> handleNoSuchThingException(PrestoErrorException ex) {
        logPrestoError(ex);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new UserFacingError(ex.getPrestoError(), tracer.currentSpan().context().traceIdString()
        ));
    }
    @ExceptionHandler({PrestoBadlyQualifiedNameException.class})
    public ResponseEntity<?> handleBadlyQualifiedNameException(PrestoBadlyQualifiedNameException ex) {
        log.error("Table name given was not fully qualified", ex);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new UserFacingError(ex.getMessage(), tracer.currentSpan().context().traceIdString()
        ));
    }

    @ExceptionHandler({PrestoNoSuchColumnException.class, PrestoInvalidQueryException.class})
    public ResponseEntity<?> handleBadQueryException(PrestoErrorException ex) {
        logPrestoError(ex);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new UserFacingError(ex.getPrestoError(), tracer.currentSpan().context().traceIdString()
        ));
    }

    @ExceptionHandler({PrestoInternalErrorException.class})
    public ResponseEntity<?> handleInternalErrorException(PrestoErrorException ex) {
        logPrestoError(ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new UserFacingError(ex.getPrestoError(), tracer.currentSpan().context().traceIdString()
        ));
    }

    @ExceptionHandler({PrestoUnexpectedHttpResponseException.class})
    public ResponseEntity<?> handlePrestoUnexpectedResponseException(PrestoUnexpectedHttpResponseException ex) {
        log.error("Unexpected response from Presto", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new UserFacingError(ex.getMessage(), tracer.currentSpan().context().traceIdString()
        ));
    }

    @ExceptionHandler({PrestoIOException.class})
    public ResponseEntity<?> handlePrestoIOException(PrestoIOException ex) {
        log.error("Presto IO error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new UserFacingError(ex.getMessage(), tracer.currentSpan().context().traceIdString()
        ));
    }

    @ExceptionHandler({PrestoInsufficientResourcesException.class})
    public ResponseEntity<?> handleInternalErrorException(PrestoInsufficientResourcesException ex) {
        logPrestoError(ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new UserFacingError(ex.getPrestoError(), tracer.currentSpan().context().traceIdString()
        ));
    }

    @ExceptionHandler({QueryParsingException.class})
    public ResponseEntity<?> handleQueryParsingException(QueryParsingException qex) {
        log.error("query parsing error  ", qex);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new UserFacingError("Unable to parse query", tracer.currentSpan().context().traceIdString()));
    }

    @ExceptionHandler({InvalidQueryJobException.class})
    public ResponseEntity<?> handleInvalidQueryJobException(InvalidQueryJobException iqje) {
        log.error("Invalid query job "+iqje.getQueryJobId());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new UserFacingError("The query corresponding to this search could not be found ("+iqje.getQueryJobId()+")", tracer.currentSpan().context().traceIdString()));
    }

    @ExceptionHandler({UncheckedIOException.class})
    public ResponseEntity<?> handleInternalErrorException(UncheckedIOException ex) {

        log.error("Unknown error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new UserFacingError("Unknown error", tracer.currentSpan().context().traceIdString()));
    }

    private void logPrestoError(PrestoErrorException ex) {
        log.error(ex.toString(), ex);
    }

    private static String escapeQuotes(String s) {
        return s.replace("\"", "\\\"");
    }

}
