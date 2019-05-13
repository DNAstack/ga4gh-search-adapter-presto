package org.ga4gh.discovery.search.source.presto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.ga4gh.discovery.search.Table;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;

public class FromTablesCollectorTest {

    private Metadata metadata;
    private Map<String, TableMetadata> fromTables;

    private void givenMetadata(Map<String, Table> tables) {
        metadata = mock(Metadata.class);
        for (Entry<String, Table> entry : tables.entrySet()) {
            Mockito.when(metadata.getTableMetadata(entry.getKey()))
                    .thenReturn(new TableMetadata(entry.getValue(), ImmutableList.of()));
        }
    }

    private void givenQuery(String sql) {
        fromTables = new HashMap<String, TableMetadata>();
        Query query = (Query) new SqlParser().createStatement(sql, new ParsingOptions());
        QuerySpecification querySpec = (QuerySpecification) query.getQueryBody();
        new FromTablesCollector(metadata, fromTables).process(querySpec.getFrom().get());
    }

    @Test
    public void testNoAlias() {
        givenMetadata(
                ImmutableMap.<String, Table>builder()
                        .put("users", new Table("users", "user schema"))
                        .build());

        givenQuery("SELECT * FROM users");

        assertTableCount(1);
        assertTable("users", "users", "user schema");
    }

    @Test
    public void testAlias() {
        givenMetadata(
                ImmutableMap.<String, Table>builder()
                        .put("users", new Table("users", "user schema"))
                        .build());

        givenQuery("SELECT * FROM users u");

        assertTableCount(2);
        assertTable("users", "users", "user schema");
        assertTable("u", "users", "user schema");
    }

    @Test
    public void testMulti() {
        givenMetadata(
                ImmutableMap.<String, Table>builder()
                        .put("users", new Table("users", "user schema"))
                        .put("animals", new Table("animals", "animals schema"))
                        .build());

        givenQuery("SELECT * FROM users u, animals a");

        assertTableCount(4);
        assertTable("users", "users", "user schema");
        assertTable("u", "users", "user schema");
        assertTable("animals", "animals", "animals schema");
        assertTable("a", "animals", "animals schema");
    }

    private void assertTableCount(int size) {
        assertEquals(size, fromTables.size());
    }

    private void assertTable(String tableKey, String tableName, String schema) {
        TableMetadata md = fromTables.get(tableKey);
        assertNotNull(md);
        assertEquals(tableName, md.getTableName());
        assertEquals(schema, md.getTableSchema());
    }
}
