package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Table;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class FromTablesCollector extends AstVisitor<Void, String> {

    private final Metadata metadata;
    private final Map<String, TableMetadata> fromTables;

    @Override
    protected Void visitJoin(Join node, String context) {
        process(node.getLeft());
        process(node.getRight());
        return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, String alias) {
        alias = node.getAlias() == null ? null : node.getAlias().getValue();
        return process(node.getRelation(), alias);
    }

    private String toTableName(QualifiedName qualifiedName) {
        checkArgument(
                qualifiedName.getParts().size() == 1, "only one part table names are allowed");
        return qualifiedName.getParts().get(0);
        //return qualifiedName.getParts().get(qualifiedName.getParts().size() - 1);
    }

    @Override
    protected Void visitTable(Table node, String alias) {
        String tableName = node.getName().toString(); //toTableName(node.getName());
        TableMetadata tableMetadata = metadata.getTableMetadata(tableName);
        if (alias != null) {
            TableMetadata prev = fromTables.put(alias, tableMetadata);
            checkArgument(prev == null, "Alias " + alias + " is ambiguous");
        }
        fromTables.put(tableName, tableMetadata);
        return null;
    }
}
