package com.dnastack.pgp;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.lang.reflect.Type;
import java.util.List;

import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.discovery.search.test.model.Field;
import org.ga4gh.discovery.search.test.model.ResultRow;
import org.ga4gh.discovery.search.test.model.ResultValue;
import org.ga4gh.discovery.search.test.model.SearchQueryHelper;
import org.ga4gh.discovery.search.test.model.SearchRequest;
import org.ga4gh.discovery.search.test.model.SearchResult;
import org.junit.BeforeClass;
import org.junit.Test;

import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.http.ContentType;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.mapper.factory.Jackson2ObjectMapperFactory;

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

//    @Test
    public void thereShouldBeSomeFields() {
        List<String> datasetIds =
                given().config(config)
                        .log()
                        .method()
                        .log()
                        .uri()
                        .auth()
                        .basic(requiredEnv("E2E_BASIC_USERNAME"), requiredEnv("E2E_BASIC_PASSWORD"))
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

    //@Test
    public void sqlQueryShouldFindSomething() {
        Dataset result = search("SELECT id, name FROM drs.org_ga4gh_drs.objects LIMIT 10");
        assertThat(result.getObjects(), hasSize(10));
    }

    //@Test
    public void registeredDatasetShouldHaveResultsAndSchema() {
        Dataset d = dataset("drs.org_ga4gh_drs.objects");
        //TODO: These tests are admittedly fragile.
        // However, they should serve us wll in the short term as we expect these schemas to stay more or less
        // static in terms of naming scheme/version/etc., but their sources are likely to change a lot,
        // both in terms of absoulte location as well as serving implementations.
        // Thus, we'll swallow these for now and solidify naming/versioning/etc. in the future.
        assertThat(d.getSchema().getSchemaId().toString(), equalTo("org.ga4gh.schemas.drs.v0.1.0.Object"));
        assertThat(d.getSchema().getSchemaId().getName(), equalTo("Object"));
        assertThat(d.getObjects(), not(hasSize(0)));
    }

    //@Test
    public void unregisteredDatasetShouldHaveResultsAndSchema() {
        Dataset d = dataset("postgres.public.participant");
        assertThat(d.getSchema().getSchemaId().getName(), not(equalTo(null)));
    }

    private Dataset search(String sqlQuery) {
        return search(new SearchRequest(null, sqlQuery));
    }

    private Dataset dataset(String id) {
        return given().config(config)
                .log()
                .method()
                .log()
                .uri()
                .auth()
                .basic(requiredEnv("E2E_BASIC_USERNAME"), requiredEnv("E2E_BASIC_PASSWORD"))
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
    }

    private Dataset search(SearchRequest request) {
        return given().config(config)
                .log()
                .method()
                .log()
                .uri()
                .auth()
                .basic(requiredEnv("E2E_BASIC_USERNAME"), requiredEnv("E2E_BASIC_PASSWORD"))
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
