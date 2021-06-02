package com.dnastack.ga4gh.search.adapter;

import com.dnastack.ga4gh.search.adapter.test.model.LibraryItem;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.restassured.http.ContentType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.dnastack.ga4gh.search.adapter.SearchE2eTest.getFullyQualifiedTestTableName;
import static com.dnastack.ga4gh.search.adapter.SearchE2eTest.getTestDatabaseConnection;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeThat;

@Slf4j
public class SchemaTestWithLibraryTest extends BaseE2eTest {
    private static final String INS_BASE_URI = optionalEnv("E2E_INS_BASE_URI", null);

    @BeforeClass
    public static void preflightCheck() {
        assumeThat("The base URI to the indexing service NOT DEFINED for this test", INS_BASE_URI, not(nullValue()));
    }

    @Test
    public void getTableInfo_should_returnTableAndSchema() throws JsonProcessingException {
        final String indexingServiceBearerToken = getToken(INS_BASE_URI, "ins:library:write");

        final String mockSchemaReferenceUrl = "http://search-e2e-test.dnastack.com/schema/this-does-not-exist.json";
        final String shortTableName = "libTest_" + RandomStringUtils.randomAlphanumeric(8);
        final String fullTableName = getFullyQualifiedTestTableName(shortTableName);
        createTestTable(fullTableName);
        afterThisTest(() -> deleteTable(fullTableName));

        // Test the type
        given()
            .get("/table/" + fullTableName + "/info")
            .then()
            .statusCode(200)
            .body("name", equalTo(fullTableName))
            .body("data_model.$id", not(nullValue()))
            .body("data_model.$ref", nullValue())
            .body("data_model.properties.id.type", equalTo("string"))
            .body("data_model.properties.name.type", equalTo("string"))
            .body("data_model.properties.age.type", equalTo("int")); // FIXME This is supposed to be "number".

        // Add an entry to the library table.
        final String libraryItemId = given()
            .auth().oauth2(indexingServiceBearerToken)
            .contentType(ContentType.JSON)
            .body(
                LibraryItem.builder()
                    .type("table") // private String type
                    .dataSourceName("nonexistent_connection") // private String dataSourceName
                    .dataSourceType("search:e2e:nonexistent-connection") // private String dataSourceType
                    .name(fullTableName) // private String name
                    .description("Generated by search adapter e2e test") // private String description
                    .preferredName(fullTableName) // private String preferredName
                    .aliases(List.of()) // private List<String> aliases
                    .preferredColumnNames(Map.of()) // private Map<String, String> preferredColumnNames
                    .jsonSchema(objectMapper.writeValueAsString(Map.of("$ref", mockSchemaReferenceUrl))) // private String jsonSchema
                    .size(123L) // private Long size
                    .sizeUnit("row") // private String sizeUnit
                    .dataSourceUrl("https://search-e2e-test.dnastack.com") // private String dataSourceUrl
                    .build()
            )
            .post(URI.create(INS_BASE_URI).resolve("/library"))
            .then()
            .log().ifValidationFails()
            .statusCode(200)
            .body("name", equalTo(fullTableName))
            .body("preferredName", equalTo(fullTableName))
            .extract()
            .jsonPath()
            .getString("id");
        afterThisTest(() ->
            given()
                .auth().oauth2(indexingServiceBearerToken)
                .delete(URI.create(INS_BASE_URI).resolve("/library/" + libraryItemId))
                .then()
                .statusCode(204)
        );

        given()
            .when()
            .get("/table/" + fullTableName + "/info")
            .then()
            .statusCode(200)
            .body("name", equalTo(fullTableName))
            .body("data_model.$id", nullValue())
            .body("data_model.$ref", equalTo(mockSchemaReferenceUrl))
            .body("data_model.properties", nullValue());
    }

    private static void createTestTable(String tableName) {
        final List<String> queries = List.of(
            String.format(
                // language=PostgreSQL
                "CREATE TABLE %s (id varchar(36), name varchar(255), age int)",
                tableName
            ),
            String.format(
                // language=PostgreSQL
                "INSERT INTO %s (id, name, age) VALUES ('abc123', 'Foo Bar', 123)",
                tableName
            )
        );

        try (Connection conn = getTestDatabaseConnection()) {
            queries.forEach(query -> {
                try {
                    conn.createStatement().execute(query);
                } catch (SQLException se) {
                    throw new RuntimeException("Unable to run: " + query, se);
                }
            });
        } catch (SQLException se) {
            throw new RuntimeException("Unable to connect to the server", se);
        }
    }

    private static void deleteTable(String tableName) {
        try (Connection conn = getTestDatabaseConnection()) {
            Statement statement = conn.createStatement();
            try {
                // language=PostgreSQL
                statement.execute(String.format("DROP TABLE %s", tableName));
            } catch (SQLException se) {
                throw new RuntimeException("Unable to create a test table", se);
            }
        } catch (SQLException se) {
            throw new RuntimeException("Unable to connect to the server", se);
        }
    }
}
