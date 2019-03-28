package org.ga4gh.discovery.search.query;

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
}
