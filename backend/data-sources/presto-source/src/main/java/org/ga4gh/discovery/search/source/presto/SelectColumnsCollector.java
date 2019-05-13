package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.ga4gh.discovery.search.Field;

import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SingleColumn;

public class SelectColumnsCollector extends AstVisitor<Void, Void> {

    private final Map<String, TableMetadata> fromTables;
    private final List<ResolvedColumn> selectColumns;
    private final List<TableMetadata> tables;

    public SelectColumnsCollector(
            Map<String, TableMetadata> fromTables, List<ResolvedColumn> selectColumns) {
        super();
        this.fromTables = fromTables;
        this.selectColumns = selectColumns;
        this.tables = fromTables.values().stream().distinct().collect(toList());
    }

    @Override
    protected Void visitSelect(Select node, Void context) {
        node.getChildren().forEach(this::process);
        return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, Void context) {
        selectColumns.add(resolve(node));
        return null;
    }

    private ResolvedColumn resolve(SingleColumn node) {
        Optional<String> alias = node.getAlias().map(id -> id.getValue());
        if (node.getExpression() instanceof Identifier) {
            Identifier identifier = (Identifier) node.getExpression();
            String fieldName = identifier.getValue();
            Optional<Field> uniqueField = getUniqueField(tables, fieldName);
            checkArgument(
                    uniqueField.isPresent(), "Field reference " + fieldName + " is ambiguous");
            return new ResolvedColumn(Optional.empty(), fieldName, alias, uniqueField.get());
        } else if (node.getExpression() instanceof DereferenceExpression) {
            DereferenceExpression derefExpression = (DereferenceExpression) node.getExpression();
            Identifier base = (Identifier) derefExpression.getBase();
            String fieldName = derefExpression.getField().getValue();
            String tableRef = base.getValue(); // this may be alias as well
            TableMetadata table = fromTables.get(tableRef);
            checkArgument(table != null, "Reference to undeclared table: " + tableRef);
            Field tableField = table.getField(fieldName);
            return new ResolvedColumn(Optional.of(tableRef), fieldName, alias, tableField);
        } else {
            throw new UnsupportedOperationException("only identifiers allowed in select clause");
        }
    }

    private Optional<Field> getUniqueField(List<TableMetadata> tables, String fieldName) {
        Field uniqueField = null;
        int countTablesWithField = 0;
        for (TableMetadata table : tables) {
            Optional<Field> optField = table.findField(fieldName);
            if (optField.isPresent()) {
                countTablesWithField++;
                uniqueField = optField.get();
            }
        }
        return countTablesWithField == 1 ? Optional.of(uniqueField) : Optional.empty();
    }
}
