package com.dnastack.ga4gh.search.adapter.presto;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.QualifiedName;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PrestoTable implements Comparable<PrestoTable> {

    private final String name;
    private final String schema;
    private final String prestoCatalog;
    private final String prestoSchema;
    private final String prestoTable;

    public QualifiedName getQualifiedName() {
        return QualifiedName.of(
                ImmutableList.of(
                        new Identifier(escape(prestoCatalog), true),
                        new Identifier(escape(prestoSchema), true),
                        new Identifier(escape(prestoTable), true)));
    }

    public String toQualifiedString() {
        return String.format("%s.%s.%s", prestoCatalog, prestoSchema, prestoTable);
    }

    private String escape(String identifier) {
        return "\"" + identifier + "\"";
    }

    //TODO (fizz): Should compare qualified names?
    @Override
    public int compareTo(PrestoTable o) {
        return this.name.compareTo(o.name);
    }
}
