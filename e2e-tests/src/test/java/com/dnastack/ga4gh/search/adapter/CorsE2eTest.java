package com.dnastack.ga4gh.search.adapter;

import io.restassured.RestAssured;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assume.assumeThat;

public class CorsE2eTest extends BaseE2eTest {

    private List<String> allowedCorsOrigins() {
        String config = optionalEnv("E2E_CORS_URLS", null);
        if (config == null) {
            return emptyList();
        }
        return Arrays.asList(config.split(","));
    }

    @Test
    public void corsRequest_shouldNot_return401() {
        for (String origin : allowedCorsOrigins()) {
            // @formatter:off
            given()
                .log().method()
                .log().uri()
                .header("access-control-request-method", "GET")
                .header("origin", origin)
            .when()
                .options("/tables")
            .then()
                .log().ifValidationFails()
                .statusCode(200);
            // @formatter:on
        }
    }

}
