package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.presto.exception.*;
import com.dnastack.ga4gh.search.adapter.shared.AuthRequiredException;
import com.dnastack.ga4gh.search.adapter.shared.SearchAuthRequest;
import com.dnastack.ga4gh.search.model.TableError;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.UncheckedIOException;
import java.util.Map;

@Service
public class ThrowableTransformer {
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

    public TableError transform(Throwable throwable) {
        return transform(throwable, null);
    }

    public TableError transform(Throwable throwable, String catalogName) {
        TableError error = new TableError();
        error.setTitle("Encountered an unexpected error");
        error.setStatus(getResponseStatus(throwable));
        error.setDetails(throwable.getClass().getName());
        error.setSource(catalogName);

        if (throwable instanceof AuthRequiredException) {
            SearchAuthRequest searchAuthRequest = ((AuthRequiredException) throwable).getAuthorizationRequest();
            error.setTitle("Authentication Required");
            error.setSource(searchAuthRequest.getKey());
            error.setDetails("User is not authorized to access catalog: " + searchAuthRequest.getKey()
                + ", request requires additional authorization information");
        } else if (throwable instanceof PrestoUnexpectedHttpResponseException || throwable instanceof PrestoIOException) {
            error.setTitle(throwable.getMessage());
        } else if (throwable instanceof PrestoErrorException) {
            handlePrestoError(error, (PrestoErrorException) throwable);
        } else if (throwable instanceof PrestoBadlyQualifiedNameException) {
            error.setTitle(throwable.getMessage());
        } else if (throwable instanceof InvalidQueryJobException) {
            error.setTitle("The query corresponding to this search could not be found (" + ((InvalidQueryJobException) throwable).getQueryJobId() + ")");
        } else if (throwable instanceof QueryParsingException) {
            error.setTitle("Unable to parse query");
        } else if (throwable instanceof UncheckedIOException) {
            error.setTitle("Unchecked IO Error");
            error.setDetails(throwable.getMessage());
        }

        return error;
    }

    public int getResponseStatus(Throwable throwable) {
        HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
        if (responseStatuses.containsKey(throwable.getClass())) {
            status = responseStatuses.get(throwable.getClass());
        }

        return status.value();
    }

    private void handlePrestoError(TableError error, PrestoErrorException prestoErrorException) {
        var prestoError = prestoErrorException.getPrestoError();
        error.setTitle(prestoError.getMessage());
        error.setDetails(String.format(
            "%s: %s: %s",
            prestoError.getErrorType(),
            prestoError.getErrorCode(),
            prestoError.getErrorName()
        ));
    }
}
