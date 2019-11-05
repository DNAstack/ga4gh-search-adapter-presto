package com.dnastack.ga4gh.search.adapter;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;

import com.dnastack.ga4gh.search.adapter.test.model.SearchRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.specification.RequestSpecification;
import java.net.URI;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchE2eTest extends BaseE2eTest {

    private static RestAssuredConfig config;
    private static String table;
    private static String clientId;
    private static String clientSecret;
    private static String audience;


    @BeforeClass
    public static void beforeClass() {
        table = requiredEnv("E2E_TABLE");
        clientId = requiredEnv("E2E_WALLET_CLIENT_ID");
        clientSecret = requiredEnv("E2E_WALLET_CLIENT_SECRET");
        audience = requiredEnv("E2E_WALLET_AUDIENCE");
        config =
            RestAssuredConfig.config()
                .objectMapperConfig(
                    ObjectMapperConfig.objectMapperConfig()
                        .defaultObjectMapperType(ObjectMapperType.JACKSON_2)
                        .jackson2ObjectMapperFactory(
                            (cls, charset) -> new ObjectMapper()));

    }


    private String getToken() {
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
            .formParam("audience",audience)
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


    @Test
    public void sqlQueryShouldFindSomething() {

        SearchRequest request = new SearchRequest("SELECT * FROM " + table + " LIMIT 10");
        String accessToken = getToken();

        //@formatter:off
        given().config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .oauth2(accessToken)
            .when()
            .contentType(ContentType.JSON)
            .body(request)
        .post("/search")
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .body("data.size()",greaterThan(0))
            .body("data_model", not(empty()));
        //@formatter:on
    }


    @Test
    public void listingTables_ShouldContainTargetTable() {
        String accessToken = getToken();

        //@formatter:off
        given().config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .oauth2(accessToken)
            .when()
            .contentType(ContentType.JSON)
        .get("/tables")
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .body("tables.size()",greaterThan(0))
            .body("tables.name",hasItem(table));
        //@formatter:on
    }

    @Test
    public void getTableInfo_ShouldReturnTableAndSchema() {
        String accessToken = getToken();

        //@formatter:off
        given().config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .oauth2(accessToken)
            .when()
            .contentType(ContentType.JSON)
        .get("/table/{table_name}/info",table)
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .body("name",equalTo(table))
            .body("data_model",not(empty()));
        //@formatter:on
    }

    @Test
    public void getTableData_shouldReturnDataAndDataModel() {
        String accessToken = getToken();

        //@formatter:off
        given().config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .oauth2(accessToken)
            .when()
            .contentType(ContentType.JSON)
        .get("/table/{table_name}/data",table)
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .body("data.size()",greaterThan(0))
            .body("data_model",not(empty()));
        //@formatter:on
    }

    @Test
    public void getTableData_PaginationShouldWork() {
        String accessToken = getToken();
        //@formatter:off
        ObjectNode tableData = given().config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .oauth2(accessToken)
            .when()
            .contentType(ContentType.JSON)
            .queryParam("pageSize",2)
        .get("/table/{table_name}/data",table)
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .body("data.size()",equalTo(2))
            .body("data_model",not(empty()))
            .extract()
            .as(ObjectNode.class);
        //@formatter:on

        //@formatter:off
        ObjectNode firstPage = given().config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .oauth2(accessToken)
            .when()
            .contentType(ContentType.JSON)
            .queryParam("pageSize",1)
        .get("/table/{table_name}/data",table)
            .then()
            .log()
            .ifValidationFails()
            .statusCode(200)
            .body("data.size()",equalTo(1))
            .body("data_model",not(empty()))
            .body("pagination.next_page_url",is(not(isEmptyOrNullString())))
            .extract()
            .as(ObjectNode.class);
        //@formatter:on
        String nextPageUrl = URI.create(firstPage.get("pagination").get("next_page_url").asText()).getPath();
        //@formatter:off
        ObjectNode secondPage = given().config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .oauth2(accessToken)
            .when()
            .contentType(ContentType.JSON)
            .queryParam("pageSize",2)
        .get(nextPageUrl)
            .then()
            .log()
            .ifValidationFails()
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
