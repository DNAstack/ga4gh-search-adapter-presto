package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.model.Field;
import java.util.Optional;
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
