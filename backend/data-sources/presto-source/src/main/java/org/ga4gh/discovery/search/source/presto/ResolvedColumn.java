package org.ga4gh.discovery.search.source.presto;

import java.util.Optional;

import org.ga4gh.discovery.search.Field;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ResolvedColumn {
    private final Optional<String> tableReference;
    private final String columnName;
    private final Optional<String> columnAlias;
    private final Field resolvedField;
}
