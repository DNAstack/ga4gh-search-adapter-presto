package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.security.ServiceAccountAuthenticator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.oauth2.jwt.Jwt;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class PrestoHttpClient implements PrestoClient {

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {};

    private static final String DEFAULT_PRESTO_USER_NAME = "ga4gh-search-adapter-presto";
    private static final int MAX_RETRIES = 10; //TODO: Improve how retries work

    private final String prestoServer;
    private final String prestoSearchEndpoint;
    private final ServiceAccountAuthenticator authenticator;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public PrestoHttpClient(String prestoServerUrl, ServiceAccountAuthenticator accountAuthenticator) {
        this.prestoServer = prestoServerUrl;
        this.prestoSearchEndpoint = prestoServerUrl + "/v1/statement";
        this.authenticator = accountAuthenticator;
    }

    public JsonNode query(String statement, Map<String, String> extraCredentials) throws IOException {
        try (Response response = post(prestoSearchEndpoint, statement, extraCredentials)) {
            if (!response.isSuccessful()) { //todo: need this condition?
                return null; //TODO: better failure logic (503s?)
            }

            return waitForQueryResults(response, 0, extraCredentials);
        }
    }

    public JsonNode next(String page, Map<String, String> extraCredentials) throws IOException {
        //TODO: better url construction
        try (Response response =  get(this.prestoServer + "/" + page, extraCredentials)) {
            if (response.body() == null) {
                log.info("Got null response from Presto on page {}. All done?", page);
                return null; // Got all content
            }
            JsonNode body = objectMapper.readTree(response.body().string());
            return isQuerySuccess(body) ? body : null;
        }
    }

    private boolean isQuerySuccess(JsonNode node) {
        return node.has("columns");
    }

    private Map<String, Object> extractExtraCredentialsRequest(JsonNode node) {
        JsonNode error = node.get("error");
        if (error != null &&
                Objects.equals(error.get("errorName").asText(), "RESOURCE_AUTH_REQUIRED")) {

            PrestoFailureInfo failureInfo = objectMapper.convertValue(error.get("failureInfo"), PrestoFailureInfo.class);
            String embeddedJson = failureInfo.getMessageOfCauseType("io.prestosql.spi.PrestoException");
            if (embeddedJson == null) {
                throw new RuntimeException("This looks like an auth challenge from Presto, but the auth challenge payload could not be found in " + embeddedJson);
            }
            try {
                return objectMapper.readValue(embeddedJson, MAP_TYPE_REFERENCE);
            } catch (IOException e) {
                throw new RuntimeException("The backend requested additional auth info but it couldn't be parsed as a JSON object: " + embeddedJson);
            }
        }
        return null;
    }

    /**
     * Models the structure of the "failureInfo" member that comes back from a Presto error response.
     */
    @Data
    private static class PrestoFailureInfo {
        String type;
        String message;
        PrestoFailureInfo cause;

        /**
         * Digs through the cause chain until it finds a cause with the given type, then returns the message from
         * that node.
         *
         * @param fqcn fully-qualified class name of the exception type whose message to retrieve.
         * @return the message associated with the given exception in the cause chain.
         */
        String getMessageOfCauseType(String fqcn) {
            if (type.equals(fqcn)) {
                return message;
            }
            if (cause == null) {
                return null;
            }
            return cause.getMessageOfCauseType(fqcn);
        }
    }

    private boolean isQueryFailure(JsonNode node) {
        return (node.hasNonNull("stats") &&
                node.get("stats").hasNonNull("state") &&
                node.get("stats").get("state").asText().equalsIgnoreCase("failed"))
                ||
                !node.hasNonNull("nextUri");
    }

    private JsonNode waitForQueryResults(Response currentResponse, int currentRetry, Map<String, String> extraCredentials) throws IOException {
        if (!currentResponse.isSuccessful()) {
            throw new PrestoQueryFailedException(currentResponse.code(), currentResponse.body().string());
        }

        if (currentResponse.body() == null) {
            throw new PrestoQueryFailedException(currentResponse.code(), "No response body received from backend");
        }

        JsonNode currentBody = objectMapper.readTree(currentResponse.body().string());
        if (isQuerySuccess(currentBody)) {
            return currentBody;
        }

        Map<String, Object> credentialsRequest = extractExtraCredentialsRequest(currentBody);
        if (credentialsRequest != null) {
            throw new AuthRequiredException(credentialsRequest);
        }

        if (isQueryFailure(currentBody)) {
            String errorMessage = (currentBody.hasNonNull("error")
                    && currentBody.get("error").hasNonNull("message"))
                    ? currentBody.get("error").get("message").asText()
                    : "<no message>";
            throw new PrestoQueryFailedException(currentResponse.code(), currentBody.toString());
        }

        if (currentRetry > MAX_RETRIES) {
            throw new PrestoQueryFailedException(currentResponse.code(), "Maximum backend retries exceeded");
        }

        backoff(currentRetry);
        try (Response response = get(currentBody.get("nextUri").asText(), extraCredentials)) {
            // todo don't recurse. also, close previous response before making next request.
            return waitForQueryResults(response, ++currentRetry, extraCredentials);
        }
    }

    //todo: move retry constraints to this method?
    //todo: better retry logic
    private void backoff(int retries) {
        try {
            Thread.sleep(250 * retries);
        } catch (Exception e) {
            log.warn("Oh noes", e);
        }
    }

    private Response get(String url, Map<String, String> extraCredentials) throws IOException {
        Request.Builder request = new Request.Builder()
                .url(url)
                .method("GET", null);
        return execute(request, extraCredentials);
    }

    private Response post(String url, String body, Map<String, String> extraCredentials) throws IOException {
        RequestBody requestBody = RequestBody.create(null, body);
        Request.Builder request = new Request.Builder().url(url).method("POST", requestBody);
        return execute(request, extraCredentials);
    }

    private Response execute(final Request.Builder request, Map<String, String> extraCredentials) throws IOException {
        request.header("X-Presto-User", getUserNameForRequest());
        extraCredentials.forEach((k, v) -> request.addHeader("X-Presto-Extra-Credential", k+"="+v));
        if (!authenticator.requiresAuthentication()) {
            return new OkHttpClient().newCall(request.build()).execute();
        }

        request.header("Authorization", "Bearer " + authenticator.getAccessToken());
        Response firstTry = new OkHttpClient().newCall(request.build()).execute();
        if (firstTry.code() != 401 && firstTry.code() != 403) {
            return firstTry;
        }
        firstTry.close();

        authenticator.refreshAccessToken();
        request.header("Authorization", "Bearer " + authenticator.getAccessToken());
        return new OkHttpClient().newCall(request.build()).execute();
    }

    /**
     * If the Incoming request has authentication information, use the attached user principal as the username to pass
     * to presto, otherwise, return {@link #DEFAULT_PRESTO_USER_NAME the default username}.
     */
    private String getUserNameForRequest() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated()) {
            return DEFAULT_PRESTO_USER_NAME;
        }

        Object principal = authentication.getPrincipal();
        if (principal instanceof User) {
            return ((User) principal).getUsername();
        }
        if (principal instanceof Jwt) {
            return ((Jwt) principal).getSubject();
            }

        return principal.toString();
    }
}
