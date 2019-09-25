package org.ga4gh.discovery.search.source.presto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Base64;
import lombok.NonNull;
import okhttp3.Call;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class ServiceAccountAuthenticator {

    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final String audience;
    private final String tokenEndpoint;

    private String accessToken;

    public ServiceAccountAuthenticator(@NonNull String clientId, @NonNull String clientSecret, @NonNull String scope, @NonNull String audience, @NonNull String tokenEndpoint) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.audience = audience;
        this.tokenEndpoint = tokenEndpoint;
        refreshAccessToken();
    }


    public String getAccessToken() {
        return accessToken;
    }

    public void refreshAccessToken() {
        try {
            accessToken = authorizeServiceAndRetrieveAccessToken();
        } catch (IOException e) {
            throw new ServiceAccountAuthenticationException(
                "Encountered error while authenticating service account: " + e.getMessage(), e);
        }
    }

    private String authorizeServiceAndRetrieveAccessToken() throws IOException {

        String combinedClientCredentials = clientId + ":" + clientSecret;
        String encodedClientCredentials =
            "Basic " + Base64.getEncoder().encodeToString(combinedClientCredentials.getBytes());

        FormBody formBody = new FormBody.Builder().add("grant_type", "client_credentials")
            .add("scope", scope)
            .add("audience", audience)
            .build();

        Request request = new Request.Builder().url(tokenEndpoint).method("POST", formBody)
            .header("Authorization", encodedClientCredentials).build();

        return executeRequest(request);
    }


    private String executeRequest(Request request) throws IOException {
        OkHttpClient httpClient = new OkHttpClient();
        Call call = httpClient.newCall(request);
        try (Response response = call.execute()) {
            if (response.isSuccessful() && response.body() != null) {
                ResponseBody body = response.body();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(body.string());

                if (node.has("access_token")) {
                    return node.get("access_token").asText();
                } else {
                    throw new IOException("Received successful status code but could not read access_token, property does not exist in body");
                }
            } else if (response.isSuccessful()) {
                throw new IOException("Received successful status code but could not read access_token, response does not have a body");
            } else {
                throw new ServiceAccountAuthenticationException(
                    "Could not authenticate service account, statusCode=" + response.code() + ", message=" + response
                        .message());
            }
        }
    }

}
