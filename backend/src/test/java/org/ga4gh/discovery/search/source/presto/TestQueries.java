package org.ga4gh.discovery.search.source.presto;

import java.util.Optional;

import org.ga4gh.discovery.search.model.query.SearchQueryHelper;

import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Offset;
import io.prestosql.sql.tree.Query;

public class TestQueries extends SearchQueryHelper {

    public static Query animalsQuery(Integer offset, Integer limit) {
        Optional<Offset> optOffset = offset == null ? noOffset() : offset(offset);
        Optional<Node> optLimit = limit == null ? noLimit() : limit(limit);
        return query(
                select(field("id"), field("name")),
                from(table("fact")),
                noWhere(),
                optOffset,
                optLimit);
    }
}
