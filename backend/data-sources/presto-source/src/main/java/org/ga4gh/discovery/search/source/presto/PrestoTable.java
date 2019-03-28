package org.ga4gh.discovery.search.source.presto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PrestoTable {

    private final String name;
    private final String schema;
    private final String prestoCatalog;
    private final String prestoSchema;
    private final String prestoTable;

    public String getQualifiedName() {
        return "\"" + prestoCatalog + "\".\"" + prestoSchema + "\".\"" + prestoTable + "\"";
    }
}
