package org.ga4gh.discovery.search.source.presto;

import static org.ga4gh.discovery.search.source.presto.MockPrestoMetadata.animalsMetadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.sql.SQLException;
import java.util.List;

import org.ga4gh.discovery.search.request.SearchRequest;
import org.ga4gh.discovery.search.result.ResultRow;
import org.ga4gh.discovery.search.result.SearchResult;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PrestoSearchSourceTest {

    private PrestoSearchSource searchSource;
    private SearchResult searchResult;

    private void givenDefaultTable() throws SQLException {
        MockPrestoAdapter mockPresto = new MockPrestoAdapter(animalsMetadata());
        @SuppressWarnings("resource")
        MockResultSet resultSet =
                new MockResultSet(
                                ImmutableList.of("id", "name"),
                                ImmutableList.of("integer", "varchar"))
                        .addRow("1", "Dog")
                        .addRow("2", "Cat")
                        .addRow("3", "Cow")
                        .addRow("4", "Chicken")
                        .addRow("5", "Bird")
                        .addRow("6", "Turtle")
                        .addRow("7", "Lion")
                        .addRow("8", "Zebra")
                        .addRow("9", "Horse")
                        .addRow("10", "Ant");
        mockPresto.addMockResults(
                "SELECT \"id\", \"name\"\n" + "FROM \"postgres\".\"public\".\"fact\"", resultSet);
        mockPresto.addMockResults(
                "SELECT \"id\", \"name\"\n"
                        + "FROM \"postgres\".\"public\".\"fact\"\nOFFSET 3\nLIMIT 3",
                resultSet.subset(3, 3));
        mockPresto.addMockResults(
                "SELECT \"id\", \"name\"\n" + "FROM \"postgres\".\"public\".\"fact\"\nOFFSET 3",
                resultSet.subset(3, 7));

        mockPresto.addMockResults("show catalogs", defaultCatalogs());
        mockPresto.addMockResults("SHOW SCHEMAS FROM \"postgres\"", schemaResult("public"));
        String selectTablesQuery = "select table_name, table_schema from \"postgres\".information_schema.tables WHERE table_schema NOT IN ('information_schema', 'connector_views', 'roles')";
        MockResultSet tableInfo = new MockResultSet(ImmutableList.of("table_name", "table+schema"), ImmutableList.of("varchar", "varchar"));
        tableInfo.addRow("fact", "public");
        mockPresto.addMockResults(selectTablesQuery, tableInfo);
        searchSource = new PrestoSearchSource(mockPresto);
    }

    //TODO: This is getting really bad
    private MockResultSet schemaResult(String... schema) {
         MockResultSet resultSet = new MockResultSet(
            ImmutableList.of("Schema"),
            ImmutableList.of("varchar"));
        for (String s : schema) {
            resultSet.addRow(s);
        }
        return resultSet;
    }

    private MockResultSet defaultCatalogs() {
        return new MockResultSet(
                ImmutableList.of("Catalog"),
                ImmutableList.of("varchar"))
//                .addRow("drs")
//                .addRow("bigquery-pgc-data")
                .addRow("postgres");
    }

    private void searchWithLimitOffset(Integer limit, Integer offset) {
        searchResult =
                searchSource.search(
                        new SearchRequest(TestQueries.animalsQuery(offset, limit), null));
    }

    private void assertIndices(int... indices) {
        List<ResultRow> rows = searchResult.getResults();
        assertThat(rows, is(notNullValue()));
        assertThat(rows, hasSize(indices.length));
        for (int i = 0; i < indices.length; i++) {
            assertThat(
                    rows.get(i).getValues().get(0).getValue(),
                    is(new Integer(indices[i]).toString()));
        }
    }

    @Test
    public void testNoLimitNoOffset() throws SQLException {
        givenDefaultTable();
        searchWithLimitOffset(null, null);
        assertIndices(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testNoLimitOffset3() throws SQLException {
        givenDefaultTable();
        searchWithLimitOffset(null, 3);
        assertIndices(4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testLimit3Offset3() throws SQLException {
        givenDefaultTable();
        searchWithLimitOffset(3, 3);
        assertIndices(4, 5, 6);
    }
}
