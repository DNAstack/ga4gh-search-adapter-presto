package com.dnastack.ga4gh.search.adapter.presto;

import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class PrestoCatalog {
    private final String name;
    private final List<PrestoSchema> schemas;

    public String getName() {
        return name;
    }

    public List<PrestoSchema> getSchema() {
        return Collections.unmodifiableList(schemas);
    }
}
