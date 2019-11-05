package com.dnastack.ga4gh.search.adapter.auth;

import com.dnastack.ga4gh.search.adapter.security.AuthConfig.OauthClientConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

@Slf4j
@SuppressWarnings("Duplicates")
public class ServiceAccountAuthenticator {

    private final static Long TOKEN_BUFFER = 60L;
    private final String clientId;
    private final String clientSecret;
    private final String scopes;
    private final String audience;
    private final String tokenEndpoint;

    private AuthTokenResponse tokenResponse;
    private Long tokenRetrievedAt = 0L;

    public ServiceAccountAuthenticator(OauthClientConfig config) {
        this.clientId = config.getClientId();
        this.clientSecret = config.getClientSecret();
        this.scopes = config.getScopes();
        this.audience = config.getAudience();
        this.tokenEndpoint = config.getTokenUri();
        refreshAccessToken();
    }


    public String getAccessToken() {
        Long now = Instant.now().getEpochSecond();
        if (tokenResponse == null) {
            refreshAccessToken();
        } else if (tokenResponse.getExpiresIn() != null && now > (tokenRetrievedAt + tokenResponse.getExpiresIn()
            - TOKEN_BUFFER)) {
            log.trace("Access token has expired, or will be expiring within the buffer window. Refreshing token now");
            refreshAccessToken();
        } else if (tokenResponse.getExpiresIn() == null) {
            log.trace("Token response does not have any expiry information. Optimistically assuming token is valid");
        }
        return tokenResponse.getAccessToken();
    }

    public void refreshAccessToken() {
        try {
            tokenResponse = authorizeServiceAndRetrieveAccessToken();
            tokenRetrievedAt = Instant.now().getEpochSecond();
            log.trace("Successfully retrieved access token");
        } catch (IOException e) {
            throw new ServiceAccountAuthenticationException(
                "Encountered error while authenticating service account: " + e.getMessage(), e);
        }
    }

    private AuthTokenResponse authorizeServiceAndRetrieveAccessToken() throws IOException {
        log.trace("Retrieving access token with client {} from {}", clientId, tokenEndpoint);
        String combinedClientCredentials = clientId + ":" + clientSecret;
        String encodedClientCredentials =
            "Basic " + Base64.getEncoder().encodeToString(combinedClientCredentials.getBytes());

        FormBody.Builder formBodyBuilder = new FormBody.Builder().add("grant_type", "client_credentials")
            .add("audience", audience);

        if (scopes != null && !scopes.isEmpty()) {
            formBodyBuilder.add("scope", scopes);
        }

        Request request = new Request.Builder().url(tokenEndpoint).method("POST", formBodyBuilder.build())
            .header("Authorization", encodedClientCredentials).build();

        return executeRequest(request);
    }


    private AuthTokenResponse executeRequest(Request request) throws IOException {
        OkHttpClient httpClient = new OkHttpClient();
        Call call = httpClient.newCall(request);
        try (Response response = call.execute()) {
            if (response.isSuccessful() && response.body() != null) {
                ResponseBody body = response.body();
                ObjectMapper mapper = new ObjectMapper();
                String bodyString = body.string();
                JsonNode node = mapper.readTree(bodyString);

                if (node.has("access_token")) {
                    return mapper.readValue(bodyString, AuthTokenResponse.class);
                } else {
                    throw new IOException("Received successful status code but could not read access_token, property does not exist in body");
                }
            } else if (response.isSuccessful()) {
                throw new IOException("Received successful status code but could not read access_token, response does not have a body");
            } else {
                String message;
                if (response.body() != null) {
                    message = response.body().string();
                } else {
                    message = response.message();
                }
                throw new ServiceAccountAuthenticationException(
                    "Could not authenticate service account, statusCode=" + response.code() + ", message=" + message);
            }
        }
    }
}