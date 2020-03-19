package com.dnastack.ga4gh.search.adapter;

import com.dnastack.ga4gh.search.adapter.test.model.ListTableResponse;
import com.dnastack.ga4gh.search.adapter.test.model.SearchRequest;
import com.dnastack.ga4gh.search.adapter.test.model.Table;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.http.ContentType;
import io.restassured.http.Method;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

@Slf4j
public class SearchE2eTest extends BaseE2eTest {

    @Test
    public void sqlQueryShouldFindSomething() throws Exception {

        SearchRequest query = new SearchRequest("SELECT * FROM " + table + " LIMIT 10");
        log.info("Running query {}", query);

        Table result = searchApiRequest(Method.POST, "/search", query, 200, Table.class);
        assertThat(result, not(nullValue()));
        assertThat(result.getDataModel().entrySet(), hasSize(greaterThan(0)));
    }

    @Test
    public void getTables_should_returnAtLeastOneTable() throws Exception {
        ListTableResponse listTableResponse = searchApiGetRequest("/tables", 200, ListTableResponse.class);
        assertThat(listTableResponse, not(nullValue()));
        assertThat(listTableResponse.getTables(), hasSize(greaterThan(0)));
    }

    @Test
    public void getTableInfo_should_returnTableAndSchema() throws Exception {
        Table tableInfo = searchApiGetRequest("/table/" + table + "/info", 200, Table.class);
        assertThat(tableInfo, not(nullValue()));
        assertThat(tableInfo.getName(), equalTo(table));
        assertThat(tableInfo.getDataModel(), not(nullValue()));
        assertThat(tableInfo.getDataModel().entrySet(), not(empty()));
    }

    @Test
    public void getTableData_should_returnDataAndDataModel() throws Exception {
        Table tableData = searchApiGetRequest("/table/" + table + "/data", 200, Table.class);
        List<Map<String, Object>> rows = searchApiGetAllPages(tableData);
        assertThat(tableData, not(nullValue()));
        assertThat(rows, not(nullValue()));
        assertThat(rows, not(empty()));
        assertThat(tableData.getDataModel(), not(nullValue()));
        assertThat(tableData.getDataModel().entrySet(), not(empty()));
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
            .get("/table/{tableName}/data", table)
        .then()
            .log().ifValidationFails()
            .statusCode(403)
            .header("WWW-Authenticate", containsString("error=\"insufficient_scope\""));
        //@formatter:on
    }

    @Ignore("Currently can't request your own page size.")
    @Test
    public void getTableData_PaginationShouldWork() throws Exception {
        //@formatter:off
        ObjectNode tableData =
                givenAuthenticatedRequest()
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
                givenAuthenticatedRequest()
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
                givenAuthenticatedRequest()
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
