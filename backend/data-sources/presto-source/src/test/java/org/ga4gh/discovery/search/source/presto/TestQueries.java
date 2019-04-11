package org.ga4gh.discovery.search.source.presto;

import java.util.OptionalLong;
import org.ga4gh.discovery.search.query.SearchQuery;

public class TestQueries extends SearchQueryHelper {

    public static SearchQuery animalsQuery(Long limit, Long offset) {
        OptionalLong optLimit =
                limit == null ? OptionalLong.empty() : OptionalLong.of(limit.longValue());
        OptionalLong optOffset =
                offset == null ? OptionalLong.empty() : OptionalLong.of(offset.longValue());
        return query(
                select(field("id"), field("name")),
                from(table("facts")),
                noWhere(),
                optLimit,
                optOffset);
    }
}
