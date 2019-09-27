package com.dnastack.ga4gh.search.adapter.presto;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class PrestoMetadata {

    private final PrestoAdapter presto;
    private final List<PrestoCatalog> catalogs;
    private final Map<String, PrestoTable> tables;
    private Map<PrestoTable, List<PrestoField>> fields;

    public List<PrestoField> getFields(PrestoTable table) {
        return fields.get(table);
    }

    public List<PrestoField> getFields() {
        //TODO: Better impl
        List<PrestoField> f = new ArrayList<>();
        fields.forEach((k, v) -> f.addAll(v));
        return f;
    }

    public List<PrestoCatalog> getCatalogs() {
        return catalogs;
    }

    public PrestoTable getPrestoTable(String tableName) {
        Optional<PrestoTable> table = findPrestoTable(tableName);
        checkArgument(
                table.isPresent(), String.format("Table %s is not a presto table", tableName));
        return table.get();
    }

    public Optional<PrestoTable> findPrestoTable(String tableName) {
        return Optional.ofNullable(tables.get(tableName));
    }

    //TODO: Fizz : Evaluate OM here
    public Map<String, PrestoTable> getTables() {
        return this.tables;
    }

    public PrestoTableMetadata getTableMetadata(String tableName) {
        PrestoTable prestoTable = tables.get(tableName);
        checkArgument(prestoTable != null, "Unknown table: " + tableName);
        return presto.getMetadata(prestoTable);
    }
}
