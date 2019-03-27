package com.dnastack.pgp;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import java.util.List;
import org.junit.Test;

public class FieldsE2eTest extends BaseE2eTest {

    @Test
    public void thereShouldBeSomeFields() {
        List<String> datasetIds =
                given().log()
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
}
