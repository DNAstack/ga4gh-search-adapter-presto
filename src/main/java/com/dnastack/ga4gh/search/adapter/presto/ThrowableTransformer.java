package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.presto.exception.*;
import com.dnastack.ga4gh.search.adapter.shared.AuthRequiredException;
import com.dnastack.ga4gh.search.adapter.shared.SearchAuthRequest;
import com.dnastack.ga4gh.search.model.TableError;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class ThrowableTransformer {
    public TableError transform(Throwable throwable) {
        return transform(throwable, null);
    }

    public TableError transform(Throwable throwable, String catalogName) {
        TableError error = new TableError();
        error.setMessage("Encountered an unexpected error");
        error.setSource(catalogName);
        error.setCode(TableError.ErrorCode.ERROR_RESPONSE);
        error.setAttributes(new HashMap<>());

        if (throwable instanceof AuthRequiredException) {
            SearchAuthRequest searchAuthRequest = ((AuthRequiredException) throwable).getAuthorizationRequest();
            error.setMessage("User is not authorized to access catalog: " + searchAuthRequest.getKey()
                + ", request requires additional authorization information");
            error.setSource(searchAuthRequest.getKey());
            error.setCode(TableError.ErrorCode.AUTH_CHALLENGE);
            error.getAttributes().put("resourceDescription", searchAuthRequest.getResourceDescription());
        } else if (throwable instanceof PrestoUnexpectedHttpResponseException || throwable instanceof PrestoIOException) {
            error.setMessage("Couldn't complete query to list tables"); // TODO Change to "Presto error"
            error.setCode(TableError.ErrorCode.PRESTO_QUERY); // TODO Change to ERROR_RESPONSE
        } else if (throwable instanceof PrestoErrorException) {
            handlePrestoError(error, (PrestoErrorException) throwable);
        } else if (throwable instanceof PrestoBadlyQualifiedNameException) {
            error.setMessage(throwable.getMessage());
        } else if (throwable instanceof InvalidQueryJobException) {
            error.setMessage("The query corresponding to this search could not be found (" + ((InvalidQueryJobException) throwable).getQueryJobId() + ")");
        }

        return error;
    }

    private void handlePrestoError(TableError error, PrestoErrorException prestoErrorException) {
        var prestoError = prestoErrorException.getPrestoError();
        error.setMessage(prestoError.getMessage());
        error.getAttributes().putAll(Map.of(
            "errorCode", prestoError.getErrorCode(),
            "errorName", prestoError.getErrorName(),
            "errorType", prestoError.getErrorType()
        ));
    }
}
