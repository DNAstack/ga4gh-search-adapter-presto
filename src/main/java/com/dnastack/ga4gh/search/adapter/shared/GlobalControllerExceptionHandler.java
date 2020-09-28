package com.dnastack.ga4gh.search.adapter.shared;

import brave.Tracer;
import com.dnastack.ga4gh.search.adapter.presto.ThrowableTransformer;
import com.dnastack.ga4gh.search.adapter.presto.exception.*;
import com.dnastack.ga4gh.search.model.TableData;
import com.dnastack.ga4gh.search.model.TableError;
import com.dnastack.ga4gh.search.model.TableInfo;
import com.dnastack.ga4gh.search.model.TablesList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
@Slf4j
public class GlobalControllerExceptionHandler {
    @Autowired
    private Tracer tracer;

    @Autowired
    private ThrowableTransformer throwableTransformer;

    // Note: Map.of can only have up to 10 keys.
    private static final Map<Class<?>, HttpStatus> responseStatuses = Map.of(
        AuthRequiredException.class, HttpStatus.UNAUTHORIZED,
        PrestoNoSuchCatalogException.class, HttpStatus.NOT_FOUND,
        PrestoNoSuchSchemaException.class, HttpStatus.NOT_FOUND,
        PrestoNoSuchTableException.class, HttpStatus.NOT_FOUND,
        PrestoBadlyQualifiedNameException.class, HttpStatus.NOT_FOUND,
        PrestoNoSuchColumnException.class, HttpStatus.BAD_REQUEST,
        PrestoInvalidQueryException.class, HttpStatus.BAD_REQUEST,
        PrestoInsufficientResourcesException.class, HttpStatus.SERVICE_UNAVAILABLE,
        QueryParsingException.class, HttpStatus.BAD_REQUEST,
        InvalidQueryJobException.class, HttpStatus.BAD_REQUEST
    );

    @ExceptionHandler(AuthRequiredException.class)
    public ResponseEntity<?> handleAuthRequiredException(AuthRequiredException e) {
        SearchAuthRequest cr = e.getAuthorizationRequest();
        return ResponseEntity.status(401)
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .header("WWW-Authenticate", "GA4GH-Search realm=\"" + escapeQuotes(cr.getKey()) + "\"")
            .body(Map.of("authorization-request", cr, "trace_id", tracer.currentSpan().context().traceIdString()));
    }

    @ExceptionHandler({TableApiErrorException.class})
    public ResponseEntity<?> handleTableApiErrorException(TableApiErrorException apiErrorException) {
        return reply(apiErrorException, tracer.currentSpan().context().traceIdString());
    }

    private ResponseEntity<?> reply(Throwable throwable, String traceId) {
        HttpStatus status = getResponseStatus(throwable);
        Class<?> responseClass = TableData.class;
        TableError error = null;

        if (throwable instanceof TableApiErrorException) {
            TableApiErrorException apiErrorException = (TableApiErrorException) throwable;
            status = getResponseStatus(apiErrorException.getPreviousException());
            responseClass = apiErrorException.getResponseClass();
            error = throwableTransformer.transform(apiErrorException.getPreviousException());
        } else {
            error = throwableTransformer.transform(throwable);
        }

        if (traceId != null) {
            error.getAttributes().put("traceId", traceId);
        }

        // Overwrite the error code
        if (error.getCode() == TableError.ErrorCode.PRESTO_QUERY) {
            error.setCode(TableError.ErrorCode.ERROR_RESPONSE);
        }

        // For debugging only
        // TODO Maybe remove it later.
        error.getAttributes().put("exceptionClassType", throwable instanceof TableApiErrorException
            ? ((TableApiErrorException) throwable).getPreviousException().getClass().getName()
            : throwable.getClass().getName());
        error.getAttributes().put("responseClassType", responseClass.getName());

        Object body = null;

        // TODO Clean up this part.
        if (responseClass == TableData.class) {
            body = new TableData(null, null, List.of(error), null);
        } else if (responseClass == TableInfo.class) {
            body = new TableInfo(null, null, null, List.of(error));
        } else if (responseClass == TablesList.class) {
            body = new TableInfo(null, null,null, List.of(error));
        } else {
            throw new RuntimeException("Unknown response class type", throwable);
        }

        return ResponseEntity.status(status).body(body);
    }

    private HttpStatus getResponseStatus(Throwable throwable) {
        if (responseStatuses.containsKey(throwable.getClass())) {
            return responseStatuses.get(throwable.getClass());
        }

        return HttpStatus.INTERNAL_SERVER_ERROR;
    }

    private void logPrestoError(PrestoErrorException ex) {
        log.error(ex.toString(), ex);
    }

    private static String escapeQuotes(String s) {
        return s.replace("\"", "\\\"");
    }

}
