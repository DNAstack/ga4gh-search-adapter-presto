package org.ga4gh.discovery.search.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Limit;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;

public class ParserTest {

    @Test
    public void testParserOffsetLimit() {
        Query query = parse("SELECT * FROM users OFFSET 3 LIMIT 5");
        QuerySpecification querySpec = (QuerySpecification) query.getQueryBody();
        Limit limit = (Limit) querySpec.getLimit().get();
        Offset offset = querySpec.getOffset().get();
        assertEquals("3", offset.getRowCount());
        assertEquals("5", limit.getLimit());
        // in query limit and offset aren't present, they are in query spec only
        assertFalse(query.getLimit().isPresent());
        assertFalse(query.getOffset().isPresent());
    }

    @Test
    public void testParserLimitOffset() {
        Assertions.assertThatThrownBy(
                        () -> {
                            parse("SELECT * FROM users LIMIT 4 OFFSET 8");
                        })
                .isInstanceOf(ParsingException.class);
    }

    private Query parse(String sql) {
        return (Query) new SqlParser().createStatement(sql, new ParsingOptions());
    }
}
