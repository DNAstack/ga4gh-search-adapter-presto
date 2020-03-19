package com.dnastack.ga4gh.search.adapter;

import com.dnastack.ga4gh.search.adapter.test.model.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;
import io.restassured.http.Method;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.function.Supplier;

import static io.restassured.RestAssured.given;
import static io.restassured.http.Method.GET;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@Slf4j
public class BaseE2eTest {

    private static final int MAX_REAUTH_ATTEMPTS = 10;

    static RestAssuredConfig config;
    static String walletClientId;
    static String walletClientSecret;
    static String audience;
    static String table;

    /**
     * Lazily initialized if Google credentials are needed by the test.
     */
    static GoogleCredentials googleCredentials;

    /**
     * These are the extra credentials of the type that the Search API challenges for. They will be added to the
     * RestAssured requests created by {@link #givenAuthenticatedRequest(String...)}.
     */
    static Map<String, String> extraCredentials = new HashMap<>();

    @BeforeClass
    public static void beforeClass() throws Exception {
        RestAssured.baseURI = requiredEnv("E2E_BASE_URI");
        walletClientId = optionalEnv("E2E_WALLET_CLIENT_ID", null);
        walletClientSecret = optionalEnv("E2E_WALLET_CLIENT_SECRET", null);
        audience = optionalEnv("E2E_WALLET_AUDIENCE", null);
        ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        config =
                RestAssuredConfig.config()
                        .objectMapperConfig(
                                ObjectMapperConfig.objectMapperConfig()
                                        .defaultObjectMapperType(ObjectMapperType.JACKSON_2)
                                        .jackson2ObjectMapperFactory((cls, charset) -> objectMapper));

        table = lazyOptionalEnv("E2E_TABLE", BaseE2eTest::discoverTableName);
    }

    @Before
    public final void beforeEachTest() {
        extraCredentials.clear();
    }

    protected static String requiredEnv(String name) {
        String val = System.getenv(name);
        if (val == null) {
            fail("Environnment variable `" + name + "` is required");
        }
        return val;
    }

    protected static String optionalEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        if (val == null) {
            return defaultValue;
        }
        return val;
    }

    interface ExceptionalSupplier<T, E extends Exception> {
        T get() throws E;
    }
    protected static <E extends Exception> String lazyOptionalEnv(String name, ExceptionalSupplier<String, E> defaultValue) throws E {
        String val = System.getenv(name);
        if (val == null) {
            return defaultValue.get();
        }
        return val;
    }

    /**
     * Retrieves all rows of the given table by following pagination links page by page. Returns all data rows from
     * the pages visited, in order.
     *
     * @param table the table with the initial row set and pagination link.
     * @return A list starting with all the {@link Table#getData() data rows} from the given table, concatenated with
     * all data rows from the subsequent tables. Never null.
     */
    static List<Map<String, Object>> searchApiGetAllPages(Table table) throws IOException {
        List<Map<String, Object>> allRows = new ArrayList<>();
        for (int pageNum = 0; ; pageNum++) {
            allRows.addAll(table.getData());
            log.info("Found {} rows on page {}; running total {}", table.getData().size(), pageNum, allRows.size());

            URI nextPageUrl = table.getPagination().getNextPageUrl();
            if (nextPageUrl == null) {
                log.info("Total pages fetched: {}", pageNum + 1);
                break;
            }

            table = searchApiGetRequest(nextPageUrl.toString(), 200, Table.class);
        }
        return allRows;
    }

    /**
     * Performs a GET request with the currently configured authentication settings (both bearer tokens and extra
     * credentials requested by the Search API within the current test method). GA4GH Search API credential challenges
     * are handled automatically, and each challenge is validated.
     *
     * @param path path and query parameters relative to E2E_BASE_URI, or any fully-qualified URL (useful for pagination
     *            links)
     * @param expectedStatus the HTTP status the server must respond with
     * @param responseType the Java type to map the response body into (using Jackson)
     * @return the server response body mapped to the given type
     * @throws IOException if the HTTP request or JSON body parsing/mapping fails
     * @throws AssertionError if the HTTP response code does not match {@code expectedStatus} (except in the case of
     * well-formed Search API credentials challenges from the server, which are automatically retried).
     */
    static <T> T searchApiGetRequest(String path, int expectedStatus, Class<T> responseType) throws IOException {
        return searchApiRequest(GET, path, null, expectedStatus, responseType);
    }

    /**
     * Performs an HTTP request with the currently configured authentication settings (both bearer tokens and extra
     * credentials requested by the Search API within the current test method). GA4GH Search API credential challenges
     * are handled automatically, and each challenge is validated.
     *
     * @param method the HTTP method to use with the request
     * @param path path and query parameters relative to E2E_BASE_URI, or any fully-qualified URL (useful for pagination
     *            links)
     * @param body the body to send with the request. If non-null, a JSON Content-Type header will be sent and the
     *            request body will be the Jackson serialization of the given object. If null, no Content-Type and no
     *            body will be sent.
     * @param expectedStatus the HTTP status the server must respond with
     * @param responseType the Java type to map the response body into (using Jackson)
     * @return the server response body mapped to the given type
     * @throws IOException if the HTTP request or JSON body parsing/mapping fails for either the request or the response.
     * @throws AssertionError if the HTTP response code does not match {@code expectedStatus} (except in the case of
     * well-formed Search API credentials challenges from the server, which are automatically retried).
     */
    static <T> T searchApiRequest(Method method, String path, Object body, int expectedStatus, Class<T> responseType) throws IOException {
        if (expectedStatus == 401) {
            fail("This method handles auth challenges and retries on 401. You can't use it when you want a 401 response.");
        }

        Optional<HttpAuthChallenge> wwwAuthenticate;
        for (int attempt = 0; attempt < MAX_REAUTH_ATTEMPTS; attempt++) {

            // this request includes all the extra credentials we have been challenged for so far
            RequestSpecification requestSpec = givenAuthenticatedRequest();
            if (body != null) {
                requestSpec
                        .contentType(ContentType.JSON)
                        .body(body);
            }
            Response response = requestSpec.request(method, path);

            if (response.getStatusCode() == 401) {
                wwwAuthenticate = extractAuthChallengeHeader(response);
                log.info("Got auth challenge header {}", wwwAuthenticate);
                if (!wwwAuthenticate.isPresent()) {
                    throw new AssertionError("Got HTTP 401 without WWW-Authenticate header");
                }

                assertThat(wwwAuthenticate.get().getScheme(), is("GA4GH-Search"));

                SearchAuthChallengeBody challengeBody = response.as(SearchAuthChallengeBody.class);
                SearchAuthRequest searchAuthRequest = challengeBody.getAuthorizationRequest();

                assertAuthChallengeIsValid(wwwAuthenticate.get(), searchAuthRequest);
                String token = supplyCredential(searchAuthRequest);

                String existingCredential = extraCredentials.put(searchAuthRequest.getKey(), token);

                assertThat("Got re-challenged for the same credential " + searchAuthRequest + ". Is the token bad or expired?",
                        existingCredential, nullValue());
                continue;
            } else if (response.getStatusCode() == expectedStatus) {
                return response.then()
                        .log().ifValidationFails(LogDetail.ALL)
                        .extract().as(responseType);
            } else {
                response.then()
                        .log().all();
                throw new AssertionError("Unexpected response status " + response.getStatusCode() +
                        " (sent " + method + " " + path + ", expecting " + expectedStatus + " with a body that maps to " + responseType.getName() + ")");
            }
        }
        throw new AssertionError(
                "Exceeded MAX_REAUTH_ATTEMPTS (" + MAX_REAUTH_ATTEMPTS + ")." +
                " Tokens gathered so far: " + extraCredentials.keySet());
    }

    private static void assertAuthChallengeIsValid(HttpAuthChallenge wwwAuthenticate, SearchAuthRequest searchAuthRequest) {
        assertThat("Auth challenge body must contain an authorization-request but it was " + searchAuthRequest,
                searchAuthRequest, not(nullValue()));
        assertThat("Key must be present in auth request",
                searchAuthRequest.getKey(), not(nullValue()));
        assertThat("Key must match realm in auth challenge header",
                wwwAuthenticate.getParams().get("realm"), is(searchAuthRequest.getKey()));
        assertThat("Resource must be described in auth request",
                searchAuthRequest.getResourceDescription(), not(nullValue()));
    }

    private static String supplyCredential(SearchAuthRequest searchAuthRequest) throws IOException {
        log.info("Handling auth challenge {}", searchAuthRequest);

        // first check for a configured token
        // a real client wouldn't use the key to decide what to get; that would complect the client with catalog naming choices!
        // a real client should do a credential lookup using the type and resource-description!
        String tokenEnvName = "E2E_SEARCH_CREDENTIALS_" + searchAuthRequest.getKey().toUpperCase();
        String configuredToken = optionalEnv(tokenEnvName, null);
        if (configuredToken != null) {
            log.info("Using {} to satisfy auth challenge", tokenEnvName);
            return configuredToken;
        }

        if (searchAuthRequest.getResourceType().equals("bigquery")) {
            log.info("Using Google Application Default credentials to satisfy auth challenge");
            return getGoogleCredentials().getAccessToken().getTokenValue();
        }

        throw new RuntimeException("Can't satisfy auth challenge " + searchAuthRequest + ": unknown resource type. Try defining " + tokenEnvName + ".");
    }

    private static GoogleCredentials getGoogleCredentials() throws IOException {
        if (googleCredentials == null) {
            googleCredentials = GoogleCredentials.getApplicationDefault();
            googleCredentials.refresh();
        }
        return googleCredentials;
    }

    private static Optional<HttpAuthChallenge> extractAuthChallengeHeader(Response response) {
        String authChallengeString = response.header("WWW-Authenticate");
        if (authChallengeString != null) {
            try {
                return Optional.of(HttpAuthChallenge.fromString(authChallengeString));
            } catch (final Exception e) {
                throw new AssertionError("Failed to parse WWW-Authenticate header [" + authChallengeString + "]", e);
            }
        }
        return Optional.empty();
    }

    static RequestSpecification givenAuthenticatedRequest(String... scopes) {
        RequestSpecification req = given()
                .config(SearchE2eTest.config)
                .log().method()
                .log().uri();

        // add auth if configured
        if (SearchE2eTest.walletClientId != null && SearchE2eTest.walletClientSecret != null) {
            String accessToken = SearchE2eTest.getToken(scopes);
            req.auth().oauth2(accessToken);
            if (Boolean.parseBoolean(optionalEnv("E2E_LOG_TOKENS", "false"))) {
                log.info("Using access token {}", accessToken);
            }
        }

        // add extra credentials
        extraCredentials.forEach((k, v) -> req.header("GA4GH-Search-Authorization", k + "=" + v));

        return req;
    }

    static String discoverTableName() throws IOException {
        ListTableResponse listTableResponse = searchApiGetRequest("/tables", 200, ListTableResponse.class);
        assertThat(listTableResponse.getTables(), hasSize(greaterThan(0)));
        return listTableResponse.getTables().get(0).getName();
    }

    static String getToken(String... scopes) {
        RequestSpecification specification = new RequestSpecBuilder().setBaseUri(requiredEnv("E2E_WALLET_TOKEN_URI"))
                .build();

        //@formatter:off
        RequestSpecification requestSpecification = given(specification)
                .config(config)
                .log().method()
                .log().all()
                .auth().basic(walletClientId,walletClientSecret)
                .formParam("grant_type","client_credentials")
                .formParam("client_id", walletClientId)
                .formParam("client_secret", walletClientSecret)
                .formParam("audience",audience);
        if (scopes.length > 0) {
            requestSpecification.formParam("scope", String.join(" ", scopes));
        }
        JsonPath tokenResponse = requestSpecification
            .when()
                .post()
            .then()
                .log().ifValidationFails()
                .statusCode(200)
                .extract().jsonPath();
        //@formatter:on

        return tokenResponse.getString("access_token");
    }

}
