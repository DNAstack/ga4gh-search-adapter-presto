package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.security.ServiceAccountAuthenticator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@Slf4j
public class PrestoHttpClient implements PrestoClient {

    private static final String DEFAULT_PRESTO_USER_NAME = "ga4gh-search-adapter-presto";
    private static final int MAX_RETRIES = 10; //TODO: Improve how retries work

    private final String prestoServer;
    private final String prestoSearchEndpoint;
    private final ServiceAccountAuthenticator authenticator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PrestoHttpClient(String prestoServerUrl, ServiceAccountAuthenticator accountAuthenticator) {
        this.prestoServer = prestoServerUrl;
        this.prestoSearchEndpoint = prestoServerUrl + "/v1/statement";
        this.authenticator = accountAuthenticator;
    }

    public JsonNode query(String statement) {
        try (Response response = post(prestoSearchEndpoint, statement)) {
            if (!response.isSuccessful()) { //todo: need this condition?
                return null; //TODO: better failure logic (503s?)
            }

            return waitForQueryResults(response, 0);
        } catch (IOException e) {
            log.error("Unable to retrieve query results from presto: " + e.getMessage());
        }

        return null;
    }

    public JsonNode next(String page) {
        //TODO: better url construction
        try (Response response =  get(this.prestoServer + "/" + page)) {
            if (response.body() == null) {
                return null; // Got all content
            }
            JsonNode body = objectMapper.readTree(response.body().toString()); // TODO: Could NPE
            return isQuerySuccess(body) ? body : null;
        } catch (IOException e) {
            return null; //todo: better
        }
    }

    private boolean isQuerySuccess(JsonNode node) {
        return node.has("columns");
    }

    private boolean isQueryFailure(JsonNode node) {
        return (node.hasNonNull("stats") &&
                node.get("stats").hasNonNull("state") &&
                node.get("stats").get("state").asText().equalsIgnoreCase("failed"))
                ||
                !node.hasNonNull("nextUri");
    }

    private JsonNode waitForQueryResults(Response currentResponse, int currentRetry) throws IOException {
        if (!currentResponse.isSuccessful() || currentResponse.body() == null) {
            return null; //TODO: better
        }

        JsonNode currentBody = objectMapper.readTree(currentResponse.body().string());
        if (isQuerySuccess(currentBody)) {
            return currentBody;
        }

        if (isQueryFailure(currentBody)) {
            return null;
        }

        if (currentRetry > MAX_RETRIES) {
            //TODO: better return (--> "Query timed out!" ?)
            return null;
        }

        backoff(currentRetry);
        try (Response response = get(currentBody.get("nextUri").asText())) {
            return waitForQueryResults(response, ++currentRetry);
        }
    }

    //todo: move retry constraints to this method?
    //todo: better retry logic
    private void backoff(int retries) {
        try {
            Thread.sleep(250 * retries);
        } catch (Exception e) {
            log.warn("Oh noes");
        }
    }

    private Response get(String url) throws IOException {
        Request.Builder request = new Request.Builder()
                .url(url)
                .method("GET", null);
        return execute(request);
    }

    private Response post(String url, String body) throws IOException {
        RequestBody requestBody = RequestBody.create(null, body);
        Request.Builder request = new Request.Builder().url(url).method("POST", requestBody);
        return execute(request);
    }

    private Response execute(Request.Builder request) throws IOException {
        request.header("X-Presto-User", getUserNameForRequest());
        if (!authenticator.requiresAuthentication()) {
            return new OkHttpClient().newCall(request.build()).execute();
        }

        request = request.header("Authorization", "Bearer " + authenticator.getAccessToken());
        Response firstTry = new OkHttpClient().newCall(request.build()).execute();
        if (firstTry.code() != 401 && firstTry.code() != 403) {
            return firstTry;
        }

        authenticator.refreshAccessToken();
        request = request.header("Authorization", "Bearer " + authenticator.getAccessToken());
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
