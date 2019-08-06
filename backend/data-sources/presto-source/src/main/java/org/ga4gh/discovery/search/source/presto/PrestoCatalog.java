package org.ga4gh.discovery.search.source.presto;

import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class PrestoCatalog {
    private String name;
    private List<PrestoSchema> schemas;

    public String getName() {
        return name;
    }

    public List<PrestoSchema> getSchema() {
        return schemas;
    }
}
