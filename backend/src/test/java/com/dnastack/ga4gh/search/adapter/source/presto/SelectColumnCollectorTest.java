package com.dnastack.ga4gh.search.adapter.source.presto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Table;
import com.dnastack.ga4gh.search.adapter.model.Type;
import com.dnastack.ga4gh.search.adapter.presto.FromTablesCollector;
import com.dnastack.ga4gh.search.adapter.presto.Metadata;
import com.dnastack.ga4gh.search.adapter.presto.ResolvedColumn;
import com.dnastack.ga4gh.search.adapter.presto.SelectColumnsCollector;
import com.dnastack.ga4gh.search.adapter.presto.TableMetadata;
import org.junit.Test;
import org.mockito.Mockito;

public class SelectColumnCollectorTest {

    private static final TableMetadata USERS =
            new TableMetadata(
                    new Table("users", "user schema"),
                    ImmutableList.of(field("users.name"), field("users.email")));

    private static final Map<String, TableMetadata> METADATA_USERS =
            ImmutableMap.<String, TableMetadata>builder().put("users", USERS).build();

    private static final Optional<String> NO_TABLE = Optional.empty();
    private static final String NO_ALIAS = null;

    private Metadata metadata;
    private Map<String, TableMetadata> fromTables;
    private List<ResolvedColumn> selectColumns;

    private void givenMetadata(Map<String, TableMetadata> tables) {
        metadata = mock(Metadata.class);
        for (Entry<String, TableMetadata> entry : tables.entrySet()) {
            Mockito.when(metadata.getTableMetadata(entry.getKey())).thenReturn(entry.getValue());
        }
    }

    private void givenQuery(String sql) {
        fromTables = new HashMap<String, TableMetadata>();
        selectColumns = new ArrayList<>();
        Query query = (Query) new SqlParser().createStatement(sql, new ParsingOptions());
        QuerySpecification querySpec = (QuerySpecification) query.getQueryBody();
        new FromTablesCollector(metadata, fromTables).process(querySpec.getFrom().get());
        new SelectColumnsCollector(fromTables, selectColumns).process(querySpec.getSelect());
    }

    @Test
    public void testSelectAll() {
        givenMetadata(METADATA_USERS);

        givenQuery("SELECT * FROM users");

        assertColumnCount(0);
    }

    @Test
    public void testSelectOneColumn() {
        givenMetadata(METADATA_USERS);

        givenQuery("SELECT name FROM users");

        assertColumnCount(1);
        assertColumn(NO_TABLE, "name", NO_ALIAS);
    }

    @Test
    public void testSelectOneColumnWithAlias() {
        givenMetadata(METADATA_USERS);

        givenQuery("SELECT name as \"Name\" FROM users");

        assertColumnCount(1);
        assertColumn(NO_TABLE, "name", "Name");
    }

    @Test
    public void testSelectMultipleColumns() {
        givenMetadata(METADATA_USERS);

        givenQuery("SELECT name as \"Name\", email FROM users");

        assertColumnCount(2);
        assertColumn(NO_TABLE, "name", "Name");
        assertColumn(NO_TABLE, "email", NO_ALIAS);
    }

    private void assertColumnCount(int size) {
        assertEquals(size, selectColumns.size());
    }

    private boolean match(
            ResolvedColumn resolvedColumn,
            Optional<String> table,
            String column,
            Optional<String> alias) {
        return resolvedColumn.getTableReference().equals(table)
                && resolvedColumn.getColumnName().equals(column)
                && resolvedColumn.getColumnAlias().equals(alias);
    }

    private void assertColumn(Optional<String> table, String column, String alias) {
        assertTrue(
                selectColumns.stream()
                        .filter(col -> match(col, table, column, Optional.ofNullable(alias)))
                        .findAny()
                        .isPresent());
    }

    private static Field field(String name) {
        String[] t = name.split("\\.");
        return new Field(name, t[1], Type.STRING, new String[0], new String[0], t[0]);
    }
}
