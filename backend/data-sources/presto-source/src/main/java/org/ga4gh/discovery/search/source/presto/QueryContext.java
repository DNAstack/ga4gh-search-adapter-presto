package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.query.SearchQueryField;
import org.ga4gh.discovery.search.query.SearchQueryTable;

public class QueryContext {

    private final SearchQuery query;
    private final Metadata metadata;
    private final Map<String, TableMetadata> fromTables = new HashMap<String, TableMetadata>();
    private final List<ResolvedColumn> selectColumns = new ArrayList<>();

    public QueryContext(SearchQuery query, Metadata metadata) {
        this.query = query;
        this.metadata = metadata;
        init();
    }

    private void init() {
        for (SearchQueryTable table : query.getFrom()) {
            String tableName = table.getTableName();
            TableMetadata tableMetadata = metadata.getTableMetadata(tableName);
            table.getAlias()
                    .ifPresent(
                            alias -> {
                                TableMetadata prev = fromTables.put(alias, tableMetadata);
                                checkArgument(prev == null, "Alias " + alias + " is ambiguous");
                            });
            fromTables.put(tableName, tableMetadata);
        }
        List<TableMetadata> tables = fromTables.values().stream().distinct().collect(toList());
        for (SearchQueryField field : query.getSelect()) {
            if (field.getTableReference().isPresent()) {
                String tableRef = field.getTableReference().get(); // this may be alias as well
                TableMetadata table = fromTables.get(tableRef);
                checkArgument(table != null, "Reference to undeclared table: " + tableRef);
                Field tableField = table.getField(field.getName());
                selectColumns.add(new ResolvedColumn(field, tableField));
            } else {
                Optional<Field> uniqueField = getUniqueField(tables, field.getName());
                checkArgument(
                        uniqueField.isPresent(), "Field reference " + field + " is ambiguous");
                selectColumns.add(new ResolvedColumn(field, uniqueField.get()));
            }
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


    public int getSelectColumnCount() {
        return selectColumns.size();
    }

    public ResolvedColumn getSelectColumn(int i) {
        return selectColumns.get(i);
    }

    public List<Field> getSelectFields() {
        return selectColumns.stream().map(col -> col.getResolvedField()).collect(toList());
    }

    public boolean hasFromTable(String tableName) {
        return fromTables.containsKey(tableName);
    }
}
