package com.dnastack.pgp;

import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.mapper.factory.Jackson2ObjectMapperFactory;
import io.restassured.specification.RequestSpecification;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.discovery.search.test.model.SearchQueryHelper;
import org.ga4gh.discovery.search.test.model.SearchRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SearchE2eTest extends BaseE2eTest {

    private static RestAssuredConfig config;


    @BeforeClass
    public static void beforeClass() {
        config =
                RestAssuredConfig.config()
                        .objectMapperConfig(
                                ObjectMapperConfig.objectMapperConfig()
                                        .defaultObjectMapperType(ObjectMapperType.JACKSON_2)
                                        .jackson2ObjectMapperFactory(
                                                new Jackson2ObjectMapperFactory() {

                                                    @Override
                                                    public com.fasterxml.jackson.databind
                                                                    .ObjectMapper
                                                            create(Type cls, String charset) {
                                                        return SearchQueryHelper.objectMapper();
                                                    }
                                                }));

    }


    private String getToken(){
        RequestSpecification specification = new RequestSpecBuilder().setBaseUri(requiredEnv("E2E_WALLET_TOKEN_URI")).build();

        //@formatter:off
        return given(specification)
            .config(config)
            .log()
            .method()
            .log()
            .uri()
            .auth()
            .basic(requiredEnv("E2E_WALLET_CLIENT_ID"),requiredEnv("E2E_WALLET_CLIENT_SECRET"))
            .formParam("grant_type","client_credentials")
            .formParam("audience",requiredEnv("E2E_WALLET_AUDIENCE"))
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

//    @Test
    public void thereShouldBeSomeFields() {
        List<String> datasetIds =
                given().config(config)
                        .log()
                        .method()
                        .log()
                        .uri()
                        .auth()
                        .oauth2(getToken())
                        .when()
                        .get("/api/fields")
                        .then()
                        .log()
                        .ifValidationFails()
                        .statusCode(200)
                        .extract()
                        .jsonPath()
                        .getList("id");

        assertThat(datasetIds, not(empty()));
    }

    @Test
    public void sqlQueryShouldFindSomething() {
        String dataset = requiredEnv("E2E_DATASET");
        Dataset result = search("SELECT id FROM " + dataset + " LIMIT 10");
        assertThat(result.getObjects(), not(hasSize(0)));
        assertThat(result.getSchema().size(), not(is(0)));
    }

    @Test
    public void datasetShouldHaveResultsAndSchema() {
        String dataset = requiredEnv("E2E_DATASET");
        Dataset result = dataset(dataset);
        assertThat(result.getObjects(), not(hasSize(0)));
        assertThat(result.getSchema().size(), not(is(0)));
    }

    private Dataset search(String sqlQuery) {
        return search(new SearchRequest(null, sqlQuery));
    }

    private Dataset dataset(String id) {
        String accessToken = getToken();
        //@formatter:off
        return given().config(config)
                .log()
                .method()
                .log()
                .uri()
                .given()
                .auth()
                .oauth2(accessToken)
                .when()
                .contentType(ContentType.JSON)
                .pathParam("id", id)
                .get("/api/datasets/{id}")
                .then()
                .log()
                .ifValidationFails()
                .statusCode(200)
                .extract()
                .as(Dataset.class);
        //@formatter:off
    }

    private Dataset search(SearchRequest request) {
        String accessToken = getToken();
        //@formatter:off
        return given().config(config)
                .log()
                .method()
                .log()
                .uri()
                .auth()
                .oauth2(accessToken)
                .when()
                .contentType(ContentType.JSON)
                .body(request)
                .post("/api/search")
                .then()
                .log()
                .ifValidationFails()
                .statusCode(200)
                .extract()
                .as(Dataset.class);
        //@formatter:off
    }

    private int getQueryLimit(Query query) {
        QuerySpecification spec = (QuerySpecification) query.getQueryBody();
        Limit limit = (Limit) spec.getLimit().get();
        return Integer.parseInt(limit.getLimit());
    }

    private int getSelectColumnCount(Query query) {
        QuerySpecification spec = (QuerySpecification) query.getQueryBody();
        return spec.getSelect().getSelectItems().size();
    }
}
