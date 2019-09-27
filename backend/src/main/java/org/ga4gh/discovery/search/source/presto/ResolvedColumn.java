package org.ga4gh.discovery.search.source.presto;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.ga4gh.discovery.search.model.Field;

@Getter
@AllArgsConstructor
public class ResolvedColumn {

    private final Optional<String> tableReference;
    private final String columnName;
    private final Optional<String> columnAlias;
    private final Field resolvedField;
}
