package com.dnastack.ga4gh.search.adapter;

import com.dnastack.ga4gh.search.adapter.test.model.SearchRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.specification.RequestSpecification;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

public class SearchE2eTest extends BaseE2eTest {

    private static RestAssuredConfig config;
    private static String clientId;
    private static String clientSecret;
    private static String audience;
    private static String scope;
    private static String table;

    @BeforeClass
    public static void beforeClass() {

        clientId = optionalEnv("E2E_WALLET_CLIENT_ID", null);
        clientSecret = optionalEnv("E2E_WALLET_CLIENT_SECRET", null);
        audience = optionalEnv("E2E_WALLET_AUDIENCE", null);
        scope = optionalEnv("E2E_WALLET_SCOPE", null);
        config =
            RestAssuredConfig.config()
                .objectMapperConfig(
                    ObjectMapperConfig.objectMapperConfig()
                        .defaultObjectMapperType(ObjectMapperType.JACKSON_2)
                        .jackson2ObjectMapperFactory(
                            (cls, charset) -> new ObjectMapper()));

        table = optionalEnv("E2E_TABLE", getE2eTable());
    }

    private static String getE2eTable() {
        //@formatter:off
        return
            getRequest()
                .contentType(ContentType.JSON)
            .when()
                    .get("/tables")
            .path("tables[0].name").toString();
        //@formatter:on

    }

    private static String getToken() {
        RequestSpecification specification = new RequestSpecBuilder().setBaseUri(requiredEnv("E2E_WALLET_TOKEN_URI"))
            .build();

        //@formatter:off
        return given(specification)
            .config(config)
            .log()
            .method()
            .log().all()
            .auth()
            .basic(clientId,clientSecret)
            .formParam("grant_type","client_credentials")
            .formParam("client_id", clientId)
            .formParam("client_secret", clientSecret)
            .formParam("audience",audience)
            .formParam("scope", scope)
        .post()
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getString("access_token");
        //@formatter:on

    }

    private static RequestSpecification getRequest() {
        if (clientId != null && clientSecret != null) {
            return given()
                .config(config)
                .log().method()
                .log().uri()
                .auth().oauth2(getToken());
        } else {
            return given()
                .config(config)
                .log().method()
                .log().uri();
        }
    }


    @Test
    public void sqlQueryShouldFindSomething() {

        SearchRequest request = new SearchRequest("SELECT * FROM " + table + " LIMIT 10");

        //@formatter:off
        getRequest()
            .contentType(ContentType.JSON)
            .body(request)
        .when()
            .post("/search")
        .then()
            .log().ifValidationFails()
            .statusCode(200)
            .body("data_model", not(empty()));
        //@formatter:on
    }

    @Test
    public void searchHistoryShouldReturnLastRequest(){
        SearchRequest request = new SearchRequest("SELECT * FROM " + table + " LIMIT 10");

        //@formatter:off
        getRequest()
            .contentType(ContentType.JSON)
            .body(request)
        .when()
            .post("/search")
        .then()
            .log().ifValidationFails()
            .statusCode(200)
            .body("data_model", not(empty()));
        //@formatter:on

        //@formatter:off
        getRequest()
            .contentType(ContentType.JSON)
            .body(request)
        .when()
            .get("/search/history")
        .then()
            .log().ifValidationFails()
            .statusCode(200)
            .body("search_history.size()", greaterThanOrEqualTo(1))
            .body("search_history[0].query",equalTo(request.getSqlQuery()));
        //@formatter:on
    }

    @Test
    public void listingTables_ShouldContainTables() {

        //@formatter:off
        getRequest()
            .contentType(ContentType.JSON)
        .when()
            .get("/tables")
        .then()
            .log().ifValidationFails()
            .statusCode(200)
            .body("tables.size()",greaterThan(0));
        //@formatter:on
    }

    @Test
    public void getTableInfo_ShouldReturnTableAndSchema() {

        //@formatter:off
        getRequest()
            .contentType(ContentType.JSON)
        .when()
            .get("/table/{table_name}/info",table)
        .then()
            .log().ifValidationFails()
            .statusCode(200)
            .body("name",equalTo(table))
            .body("data_model",not(empty()));
        //@formatter:on
    }

    @Test
    public void getTableData_shouldReturnDataAndDataModel() {

        //@formatter:off
        getRequest()
            .contentType(ContentType.JSON)
        .when()
            .get("/table/{table_name}/data",table)
        .then()
            .log().ifValidationFails()
            .statusCode(200)
            .body("data.size()",greaterThan(0))
            .body("data_model",not(empty()));
        //@formatter:on
    }

    @Test
    public void getTableData_PaginationShouldWork() {
        //@formatter:off
        ObjectNode tableData =
                getRequest()
                    .contentType(ContentType.JSON)
                    .queryParam("pageSize",2)
                .when()
                    .get("/table/{table_name}/data",table)
                .then()
                    .log().ifValidationFails()
                    .statusCode(200)
                    .body("data.size()",equalTo(2))
                    .body("data_model",not(empty()))
                    .extract()
                    .as(ObjectNode.class);
        //@formatter:on

        //@formatter:off
        ObjectNode firstPage =
                getRequest()
                    .contentType(ContentType.JSON)
                    .queryParam("pageSize",1)
                .when()
                    .get("/table/{table_name}/data",table)
                .then()
                    .log().ifValidationFails()
                    .statusCode(200)
                    .body("data.size()",equalTo(1))
                    .body("data_model",not(empty()))
                    .body("pagination.next_page_url",is(not(isEmptyOrNullString())))
                    .extract()
                    .as(ObjectNode.class);
        //@formatter:on

        String nextPageUrl = URI.create(firstPage.get("pagination").get("next_page_url").asText()).getPath();
        //@formatter:off
        ObjectNode secondPage =
                getRequest()
                    .contentType(ContentType.JSON)
                    .queryParam("pageSize",2)
                .when()
                    .get(nextPageUrl)
                .then()
                    .log().ifValidationFails()
                    .statusCode(200)
                    .body("data.size()",equalTo(1))
                    .body("data_model",not(empty()))
                    .extract()
                    .as(ObjectNode.class);
        //@formatter:on

        assertEquals(firstPage.withArray("data").get(0), tableData.withArray("data").get(0));
        assertEquals(secondPage.withArray("data").get(0), tableData.withArray("data").get(1));
    }
}
