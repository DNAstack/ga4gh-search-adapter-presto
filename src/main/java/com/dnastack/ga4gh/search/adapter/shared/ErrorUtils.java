package com.dnastack.ga4gh.search.adapter.shared;

import com.dnastack.ga4gh.search.tables.TableError;
import com.dnastack.ga4gh.search.tables.TableError.ErrorCode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class ErrorUtils {

    public static List<TableError> handleErrors(Throwable throwable) {
        List<TableError> errors = new ArrayList<>();
        if (throwable instanceof CapturedSearchException) {
            CapturedSearchException capturedSearchException = (CapturedSearchException) throwable;
            throwable = throwable.getCause();
            if (throwable instanceof AuthRequiredException) {
                SearchAuthRequest searchAuthRequest = ((AuthRequiredException) throwable).getAuthorizationRequest();
                TableError error = new TableError();
                error.setMessage("User is not authorized to access catalog: " + searchAuthRequest.getKey()
                    + ", request requires additional authorization information");
                error.setSource(searchAuthRequest.getKey());
                error.setCode(ErrorCode.AUTH_CHALLENGE);
                error.setAttributes(searchAuthRequest.getResourceDescription());
                errors.add(error);
            } else if (throwable instanceof TimeoutException) {
                TableError error = new TableError();
                error.setMessage(
                    "Request to source: " + capturedSearchException.getSource() + " had no response after " + (
                        capturedSearchException.getElapsedTime() / 1000)
                        + " seconds.");
                error.setSource(capturedSearchException.getSource());
                error.setCode(ErrorCode.NO_RESPONSE);
                errors.add(error);
            } else {
                TableError error = new TableError();
                error.setMessage(
                    "Request to source " + capturedSearchException.getSource() + " returned an unknown error: "
                        + throwable.getMessage());
                error.setSource(capturedSearchException.getSource());
                error.setCode(ErrorCode.ERROR_RESPONSE);
                errors.add(error);
            }
        } else {
            TableError error = new TableError();
            error.setMessage(
                "Request to unknown source returned an unknown error: " + throwable.getMessage());
            error.setSource(null);
            error.setCode(ErrorCode.ERROR_RESPONSE);
            errors.add(error);
        }
        return errors;

    }


}
