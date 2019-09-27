package org.ga4gh.discovery.search.source.presto;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PrestoSchema {
    private String name;

    public String getName() {
        return name;
    }
}
