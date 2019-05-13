package org.ga4gh.discovery.search.source.presto;

import com.google.common.collect.ImmutableList;

import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.QualifiedName;
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

    public QualifiedName getQualifiedName() {
        return QualifiedName.of(
                ImmutableList.of(
                        new Identifier(prestoCatalog, true),
                        new Identifier(prestoSchema, true),
                        new Identifier(prestoTable, true)));
    }
}
