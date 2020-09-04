package com.dnastack.ga4gh.search.adapter;

import com.dnastack.ga4gh.search.adapter.test.model.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpStatus;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static io.restassured.http.Method.GET;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;

@Slf4j
public class SearchE2eTest extends BaseE2eTest {

    private static final int MAX_REAUTH_ATTEMPTS = 10;

    private static String walletClientId;
    private static String walletClientSecret;
    private static String audience;

    /**
     * Lazily initialized if Google credentials are needed by the test.
     */
    private static GoogleCredentials googleCredentials;

    /**
     * These are the extra credentials of the type that the Search API challenges for. They will be added to the
     * RestAssured requests created by {@link #givenAuthenticatedRequest(String...)}.
     */
    private static Map<String, String> extraCredentials = new HashMap<>();


    private static String inMemoryCatalog;
    private static String inMemorySchema;
    private static String unqualifiedDateTimeTestTable;
    private static String unqualifiedPaginationTestTable;
    private static Map<String, Map<String, String>> expectedValues;
    private static final Map<String, String> expectedFormats = new HashMap<>();

    @BeforeClass
    public static void beforeClass() throws Exception {
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

        inMemoryCatalog = requiredEnv("E2E_INMEMORY_TESTCATALOG");
        inMemorySchema = requiredEnv("E2E_INMEMORY_TESTSCHEMA");
        unqualifiedDateTimeTestTable = requiredEnv("E2E_INMEMORY_DATETIMETESTTABLE");
        unqualifiedPaginationTestTable = requiredEnv("E2E_INMEMORY_PAGINATIONTESTTABLE");
        String[] pairs = requiredEnv("E2E_DATETIMETESTTABLE_TYPE_MAP").split(",");
        initMapFromPairs(pairs, expectedFormats);
        expectedValues = System.getenv().entrySet().stream()
                               .filter(v->v.getKey().startsWith("E2E_DATETIMETESTTABLE_VALUE_MAP"))
                               .collect(Collectors.toMap(e->getZoneFromEnvVarName(e.getKey()), e->getExpectedValuesFromEnvVarValue(e.getValue())));
    }


    private static String getZoneFromEnvVarName(String envVarName){
        return envVarName.substring("E2E_DATETIMETESTTABLE_VALUE_MAP_".length());
    }

    private static Map<String, String> getExpectedValuesFromEnvVarValue(String value){
        String[] pairs = value.split(",");
        Map<String, String> expectedValuesForZone = new HashMap<>();
        initMapFromPairs(pairs, expectedValuesForZone);
        return expectedValuesForZone;
    }

    private static void initMapFromPairs(String[] pairs, Map<String, String> m){
        for(String pair : pairs){
            String[] splitPair = pair.split("::");
            String key = splitPair[0].trim();
            String val = splitPair[1].trim();
            m.put(key, val);
        }
    }


    @Before
    public final void beforeEachTest() {
        extraCredentials.clear();
    }

    private static String getFullyQualifiedTestTableName(String tableName){
        return inMemoryCatalog+"."+inMemorySchema+"."+tableName;
    }


    private ListTableResponse getFirstPageOfTableListing() throws Exception{
        ListTableResponse listTableResponse = searchApiGetRequest("/tables", 200, ListTableResponse.class);
        assertThat(listTableResponse.getIndex(), not(nullValue()));

        for(int i = 0; i < listTableResponse.getIndex().size(); ++i) {
            assertThat(listTableResponse.getIndex().get(i).getUrl(), not(nullValue()));
            assertThat(listTableResponse.getIndex().get(i).getPage(), is(i));
        }
        return listTableResponse;
    }


    @Test
    public void datesAndTimesHaveCorrectTypes() throws IOException{
        String qualifiedTableName = getFullyQualifiedTestTableName(unqualifiedDateTimeTestTable);
        Table tableInfo = searchApiGetRequest("/table/" + qualifiedTableName + "/info", 200, Table.class);
        assertThat(tableInfo, not(nullValue()));
        assertThat(tableInfo.getName(), equalTo(qualifiedTableName));
        assertThat(tableInfo.getDataModel(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getId(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getSchema(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getProperties(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getProperties().entrySet(), not(empty()));

        expectedFormats.entrySet().stream()
                      .forEach((entry)->{
                          assertThat(tableInfo.getDataModel().getProperties(), hasKey(entry.getKey()));
                          assertThat(tableInfo.getDataModel().getProperties().get(entry.getKey()).getFormat(), is(entry.getValue()));
                          assertThat(tableInfo.getDataModel().getProperties().get(entry.getKey()).getType(), is("string"));
        });
    }

    private void assertDatesAndTimesHaveCorrectValuesForZone(String zone, Map<String, String> expectedValues) throws IOException{
        SearchRequest query = new SearchRequest(String.format("SELECT * FROM " + getFullyQualifiedTestTableName(unqualifiedDateTimeTestTable) + " WHERE zone='%s'", zone));
        log.info("Running query {}", query);

        Table result = searchApiRequest(Method.POST, "/search", query, 200, Table.class);
        result = searchApiGetAllPages(result);

        if(result.getData() == null){
            throw new RuntimeException("Expected results for query "+query.getQuery()+", but none were found.");
        }else if(result.getData().size() > 1){
            throw new RuntimeException("Found more than one test table entry for "+zone+" time zone, but only one was expected.");
        }

        assertThat(result.getDataModel(), not(nullValue()));
        assertThat(result.getDataModel().getProperties(), not(nullValue()));

        final Map<String, ColumnSchema> properties = result.getDataModel().getProperties();
        final Map<String, Object> row = result.getData().get(0);
        expectedFormats.entrySet().stream().forEach(entry->{
            String columnName = entry.getKey();
            String expectedColumnFormat = entry.getValue();
            assertThat("Expected column with format "+expectedColumnFormat+" for column "+columnName+" ("+zone+" time zone)", properties.get(columnName).getFormat(), is(expectedColumnFormat));
            assertThat("Expected column with type string for column "+columnName+" ("+zone+" time zone)", properties.get(columnName).getType(), is("string"));
            assertThat("date/time/datetime column "+columnName+" had an unexpected value ", row.get(columnName), is(expectedValues.get(columnName)));
        });
    }


//    @Test
//    public void datesAndTimesHaveCorrectValuesForDatesAndTimesInsertedWithLosAngelesTimeZone() throws IOException{
//        assertDatesAndTimesHaveCorrectValuesForZone("LosAngeles", EXPECTED_VALUES_LOSANGELES);
//    }
//
//    @Test
//    public void datesAndTimesHaveCorrectValuesForDatesAndTimesInsertedWithUTCTimeZone() throws IOException{
//        assertDatesAndTimesHaveCorrectValuesForZone("UTC", EXPECTED_VALUES_UTC);
//    }

    @Test
    public void datesAndTimesHaveCorrectValuesForDatesAndTimesInsertedWithZone() throws IOException{
        for(Map.Entry<String, Map<String, String>> e : expectedValues.entrySet()){
           log.info("Checking date and time was inserted correctly for zone "+e.getKey());
            assertDatesAndTimesHaveCorrectValuesForZone(e.getKey(), e.getValue());
        }
    }

    @Test
    public void indexIsPresentOnFirstPage() throws Exception{
        getFirstPageOfTableListing();
    }

    @Test
    public void nextPageTrailIsConsistentWithIndexOverFirst10Pages() throws Exception{
        final int MAX_PAGES_TO_TRAVERSE = 10;

        ListTableResponse currentPage = getFirstPageOfTableListing();

        List<PageIndexEntry> pageIndex = currentPage.getIndex();
        if(pageIndex.size() == 1){
            assertThat(currentPage.getPagination(), is(nullValue()));
            return;
        }

        assertThat(currentPage.getPagination(), not(nullValue()));

        //assert that the nth page has next url equal to the n+1st index.
        for(int i = 1; i < Math.min(MAX_PAGES_TO_TRAVERSE, pageIndex.size()-1); ++i) {
            currentPage = searchApiGetRequest(currentPage.getPagination().getNextPageUrl().toString(),
                                              200,
                                              ListTableResponse.class);
            //all pages with index < pageIndex.size() - 1 should have a non null valid next url.
            assertThat(currentPage.getPagination().getNextPageUrl(), not(nullValue()));
            if (i == (pageIndex.size() - 1)) {
                assertThat(currentPage.getPagination(), is(nullValue()));
            } else {
                assertThat(currentPage.getPagination().getNextPageUrl(), is(pageIndex.get(i + 1).getUrl()));
            }
        }
        if(pageIndex.size() > MAX_PAGES_TO_TRAVERSE){
            log.info("next page trail did not end after "+MAX_PAGES_TO_TRAVERSE+" requests, but was consistent with page index over that range.");
        }
    }

    @Test
    public void malformedSqlQueryShouldReturn400AndMessageAndTraceId() throws Exception{
        SearchRequest query = new SearchRequest("SELECT * FROM FROM E2ETEST LIMIT STRAWBERRY");
        log.info("Running bad query");
        UserFacingError error = searchUntilException(query, HttpStatus.SC_BAD_REQUEST);
        log.info("Got error "+error);
        assertThat(error, not(nullValue()));
        assertThat(error.getMessage(), not(nullValue()));
        assertThat(error.getTraceId(), not(nullValue()));
    }

    @Test
    public void sqlQueryWithBadColumnShouldReturn400AndMessageAndTraceId() throws Exception{
        SearchRequest query = new SearchRequest("SELECT e2etest_olywolypolywoly FROM " + getFullyQualifiedTestTableName(unqualifiedPaginationTestTable) + " LIMIT 10");
        log.info("Running bad query");
        UserFacingError error = searchUntilException(query, HttpStatus.SC_BAD_REQUEST);
        log.info("Got error "+error);
        assertThat(error, not(nullValue()));
        assertThat(error.getMessage(), not(nullValue()));
        assertThat(error.getTraceId(), not(nullValue()));
    }

    @Test
    public void sqlQueryShouldFindSomething() throws Exception {

        SearchRequest query = new SearchRequest("SELECT * FROM " + getFullyQualifiedTestTableName(unqualifiedPaginationTestTable) + " LIMIT 10");
        log.info("Running query {}", query);


        Table result = searchApiRequest(Method.POST, "/search", query, 200, Table.class);
        while(result.getPagination() != null){
            result = searchApiGetRequest(result.getPagination().getNextPageUrl().toString(), 200, Table.class);
            if(result.getDataModel() != null){
                break;
            }
        }

        assertThat(result, not(nullValue()));
        assertThat(result.getDataModel(), not(nullValue()));
        assertThat(result.getDataModel().getProperties(), not(nullValue()));
        assertThat(result.getDataModel().getProperties().entrySet(), hasSize(greaterThan(0)));
    }

    @Test
    public void getTables_should_returnAtLeastOneTable() throws Exception {
        ListTableResponse listTableResponse = searchApiGetRequest("/tables", 200, ListTableResponse.class);
        assertThat(listTableResponse, not(nullValue()));
        assertThat(listTableResponse.getTables(), hasSize(greaterThan(0)));
    }

    @Test
    public void getTableInfoWithUnknownCatalogGives404AndMessageAndTraceId() throws Exception{
        final String prestoTableWithBadCatalog = "e2etest_olywlypolywoly.public." + unqualifiedPaginationTestTable;
        UserFacingError error = searchApiGetRequest("/table/" + prestoTableWithBadCatalog + "/info", 404, UserFacingError.class);
        assertThat(error, not(nullValue()));
        assertThat(error.getMessage(), not(nullValue()));
        assertThat(error.getTraceId(), not(nullValue()));
    }

    @Test
    public void getTableInfoWithUnknownSchemaGives404AndMessageAndTraceId() throws Exception{
        final String prestoTableWithBadSchema = inMemoryCatalog + ".e2etest_olywolypolywoly." + unqualifiedPaginationTestTable;
        UserFacingError error = searchApiGetRequest("/table/" + prestoTableWithBadSchema + "/info", 404, UserFacingError.class);
        assertThat(error, not(nullValue()));
        assertThat(error.getMessage(), not(nullValue()));
        assertThat(error.getTraceId(), not(nullValue()));
    }

    @Test
    public void getTableInfoWithUnknownTableGives404AndMessageAndTraceId() throws Exception{
        final String prestoTableWithBadTable = inMemoryCatalog+"."+inMemorySchema+"."+"e2etest_olywolypolywoly";
        UserFacingError error = searchApiGetRequest("/table/" + prestoTableWithBadTable + "/info", 404, UserFacingError.class);
        assertThat(error, not(nullValue()));
        assertThat(error.getMessage(), not(nullValue()));
        assertThat(error.getTraceId(), not(nullValue()));
    }

    @Test
    public void getTableInfoWithBadlyQualifiedTableGives404AndMessageAndTraceId() throws Exception{
        final String prestoTableWithBadTable = "e2etest_olywolypolywoly";
        UserFacingError error = searchApiGetRequest("/table/" + prestoTableWithBadTable + "/info", 404, UserFacingError.class);
        assertThat(error, not(nullValue()));
        assertThat(error.getMessage(), not(nullValue()));
        assertThat(error.getTraceId(), not(nullValue()));
    }

    @Test
    public void getTableInfo_should_returnTableAndSchema() throws Exception {
        String qualifiedTable = getFullyQualifiedTestTableName(unqualifiedPaginationTestTable);
        Table tableInfo = searchApiGetRequest("/table/" + qualifiedTable + "/info", 200, Table.class);
        assertThat(tableInfo, not(nullValue()));
        assertThat(tableInfo.getName(), equalTo(qualifiedTable));
        assertThat(tableInfo.getDataModel(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getId(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getSchema(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getProperties(), not(nullValue()));
        assertThat(tableInfo.getDataModel().getProperties().entrySet(), not(empty()));
    }

    @Test
    public void getTableData_should_returnDataAndDataModel() throws Exception {
        Table tableData = searchApiGetRequest("/table/" + getFullyQualifiedTestTableName(unqualifiedPaginationTestTable) + "/data", 200, Table.class);
        assertThat(tableData, not(nullValue()));
        tableData = searchApiGetAllPages(tableData);
        assertThat(tableData.getData(), not(nullValue()));
        assertThat(tableData.getData(), not(empty()));
        assertThat(tableData.getDataModel(), not(nullValue()));
        assertThat(tableData.getDataModel().getSchema(), not(nullValue()));
        assertThat(tableData.getDataModel().getProperties(), not(nullValue()));
        assertThat(tableData.getDataModel().getProperties().entrySet(), not(empty()));
    }

    @Test
    public void getTables_should_require_readDataModel_or_readData_scope() throws Exception {
        assumeThat(walletClientId, notNullValue());
        assumeThat(walletClientSecret, notNullValue());

        //@formatter:off
        givenAuthenticatedRequest("junk_scope")
        .when()
            .get("/tables")
        .then()
            .log().ifValidationFails()
            .statusCode(403)
            .header("WWW-Authenticate", containsString("error=\"insufficient_scope\""));
        //@formatter:on
    }

    @Test
    public void getTableData_should_require_readData_scope() throws Exception {
        assumeThat(walletClientId, notNullValue());
        assumeThat(walletClientSecret, notNullValue());

        //@formatter:off
        givenAuthenticatedRequest("read:data_model") // but not read:data
        .when()
            .get("/table/{tableName}/data", getFullyQualifiedTestTableName(unqualifiedPaginationTestTable))
        .then()
            .log().ifValidationFails()
            .statusCode(403)
            .header("WWW-Authenticate", containsString("error=\"insufficient_scope\""));
        //@formatter:on
    }


    /**
     * Retrieves all rows of the given table by following pagination links page by page.
     *
     * @param table the table with the initial row set and pagination link.
     * @return A table containing the concatenation of all rows returned over all the pagination links, as well as the
     * first non-null data model encountered.  null only if original table parameter is null.
     */
    static Table searchApiGetAllPages(Table table) throws IOException {
        while(table.getPagination() != null && table.getPagination().getNextPageUrl() != null){
            String nextPageUri = table.getPagination().getNextPageUrl().toString();
            Table nextResult = searchApiGetRequest(nextPageUri, 200, Table.class);
            if(nextResult.getData() != null){
                log.info("Got "+nextResult.getData().size()+" results");
            }
            table.append(nextResult);
        }
        return table;
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
     * @param method         the HTTP method to use with the request
     * @param path           path and query parameters relative to E2E_BASE_URI, or any fully-qualified URL (useful for pagination
     *                       links)
     * @param body           the body to send with the request. If non-null, a JSON Content-Type header will be sent and the
     *                       request body will be the Jackson serialization of the given object. If null, no Content-Type and no
     *                       body will be sent.
     * @param expectedStatus the HTTP status the server must respond with
     * @param responseType   the Java type to map the response body into (using Jackson)
     * @return the server response body mapped to the given type
     * @throws IOException    if the HTTP request or JSON body parsing/mapping fails for either the request or the response.
     * @throws AssertionError if the HTTP response code does not match {@code expectedStatus} (except in the case of
     *                        well-formed Search API credentials challenges from the server, which are automatically retried).
     */
    static <T> T searchApiRequest(Method method, String path, Object body, int expectedStatus, Class<T> responseType) throws IOException {
        if (expectedStatus == 401) {
            fail("This method handles auth challenges and retries on 401. You can't use it when you want a 401 response.");
        }

        Response response = getResponse(method, path, body);

        if (response.getStatusCode() == expectedStatus) {
            return response.then().log().ifValidationFails(LogDetail.ALL).extract().as(responseType);
        } else {
            response.then().log().headers();
            throw new AssertionError("Unexpected response status " + response.getStatusCode() + " (sent " + method + " " + path + ", expecting " + expectedStatus + " with a body that maps to " + responseType
                    .getName() + ")." + " Actual response headers: " + response.headers() + "; body: " + response.asString());
        }
    }

    /**
     * Executes a search query and follows nextUri links until a response returns the HTTP error code in expectedErrorStatus.
     * If the expected status is never reached, an assertion error is thrown.
     * @return UserFacingError The error object describing the expected error.
     * @throws IOException
     */
    private static UserFacingError searchUntilException(Object query, int expectedErrorStatus) throws IOException{
        Response response = getResponse(Method.POST, "/search", query);
        if(response.getStatusCode() == HttpStatus.SC_OK){
            log.info("Got status OK after POSTing search");
            Table table = response.then().log().ifValidationFails(LogDetail.ALL).extract().as(Table.class);
            while(table.getPagination() != null && table.getPagination().getNextPageUrl() != null){
                String nextPageUri = table.getPagination().getNextPageUrl().toString();
                Response nextPageResponse = getResponse(Method.GET, nextPageUri, null);
                log.info("Looking for status "+expectedErrorStatus+" by following nextPageUri trail, most recent request returned "+nextPageResponse.getStatusCode());
                if(nextPageResponse.getStatusCode() == expectedErrorStatus){
                    return nextPageResponse.then().log().ifValidationFails(LogDetail.ALL).extract().as(UserFacingError.class);
                }else if(nextPageResponse.getStatusCode() != HttpStatus.SC_OK){
                    throw new AssertionError("Unexpected response status " + response.getStatusCode() + " (sent GET /"+nextPageUri+", expecting " + expectedErrorStatus + " or 200");
                }else {
                    table = nextPageResponse.then().log().ifValidationFails(LogDetail.ALL).extract().as(Table.class);
                }
            }
        }else if(response.getStatusCode() == expectedErrorStatus){
            return response.then().log().ifValidationFails(LogDetail.ALL).extract().as(UserFacingError.class);
        }
        throw new AssertionError("Expected to receive status "+expectedErrorStatus+" somewhere on the nextUri trail, but never found it.");
    }

    private static Response getResponse(Method method, String path, Object body) throws IOException {
        Optional<HttpAuthChallenge> wwwAuthenticate;
        for (int attempt = 0; attempt < MAX_REAUTH_ATTEMPTS; attempt++) {

            // this request includes all the extra credentials we have been challenged for so far
            String defaultScope = optionalEnv("E2E_WALLET_DEFAULT_SCOPE", null);
            RequestSpecification requestSpec =  defaultScope == null ? givenAuthenticatedRequest()
                                                                     : givenAuthenticatedRequest(defaultScope);
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

                if ("invalid_token".equals(wwwAuthenticate.get().getParams().get("error"))) {
                    log.info("Try running again with E2E_LOG_TOKENS=true to see what's wrong");
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
            } else {
                return response;
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
                .log().uri()
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
