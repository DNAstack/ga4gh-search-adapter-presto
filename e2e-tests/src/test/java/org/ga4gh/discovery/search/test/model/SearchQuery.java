package org.ga4gh.discovery.search.test.model;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SearchQuery {

    private final List<SearchQueryField> select;
    private final List<SearchQueryTable> from;
    private final Optional<Predicate> where;
    private final OptionalLong limit;
    private final OptionalLong offset;

    @Override
    public String toString() {
        return "SearchQuery{" + "select=" + select + ", from=" + from + ", where=" + where + ", limit=" + limit + ", offset=" + offset + '}';
    }
}
