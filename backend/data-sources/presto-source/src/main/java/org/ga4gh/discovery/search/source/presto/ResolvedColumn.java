package org.ga4gh.discovery.search.source.presto;

import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.query.SearchQueryField;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ResolvedColumn {
    private final SearchQueryField queryField;
    private final Field resolvedField;
}
