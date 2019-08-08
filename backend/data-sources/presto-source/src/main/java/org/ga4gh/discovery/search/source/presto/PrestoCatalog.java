package org.ga4gh.discovery.search.source.presto;

import lombok.AllArgsConstructor;

import java.util.Collections;
import java.util.List;

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
