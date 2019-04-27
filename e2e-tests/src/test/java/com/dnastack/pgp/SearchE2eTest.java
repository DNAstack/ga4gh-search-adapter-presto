package com.dnastack.pgp;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import java.lang.reflect.Type;
import java.util.List;
import org.ga4gh.discovery.search.test.model.Field;
import org.ga4gh.discovery.search.test.model.ResultRow;
import org.ga4gh.discovery.search.test.model.ResultValue;
import org.ga4gh.discovery.search.test.model.SearchQuery;
import org.ga4gh.discovery.search.test.model.SearchQueryHelper;
import org.ga4gh.discovery.search.test.model.SearchResult;
import org.junit.BeforeClass;
import org.junit.Test;
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

    @Test
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

    @Test
    public void searchShouldFindSomething() {
        SearchQuery query = SearchQueryHelper.exampleQuery();
        SearchResult result =
                given().config(config)
                        .log()
                        .method()
                        .log()
                        .uri()
                        .auth()
                        .basic(requiredEnv("E2E_BASIC_USERNAME"), requiredEnv("E2E_BASIC_PASSWORD"))
                        .when()
                        .contentType(ContentType.JSON)
                        .body(query)
                        .post("/api/search")
                        .then()
                        .log()
                        .ifValidationFails()
                        .statusCode(200)
                        .extract()
                        .as(SearchResult.class);

        assertThat(result.getFields(), hasSize(query.getSelect().size()));
        assertThat(result.getResults(), hasSize((int) query.getLimit().getAsLong()));
        ResultRow row1 = result.getResults().get(0);
        assertThat(row1.getValues(), hasSize(query.getSelect().size()));

        for (int i = 0; i < query.getSelect().size(); i++) {
            ResultValue val = row1.getValues().get(i);
            Field field = result.getFields().get(i);
            assertThat(val.getField(), is(field));
        }
    }
}
