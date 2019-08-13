package org.ga4gh.discovery.search.source.presto;

import lombok.AllArgsConstructor;
import org.ga4gh.discovery.search.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

@AllArgsConstructor
public class PrestoMetadata {

    private final PrestoAdapter presto;
    private final List<PrestoCatalog> catalogs;
//    private final Map<String, List<PrestoSchema>> schemas;
    private final Map<String, PrestoTable> tables;
    private Map<PrestoTable, List<Field>> fields;
//
//    public PrestoMetadata(PrestoAdapter presto, Map<String, PrestoTable> tables) {
//        this.tables = tables;
//        this.presto = presto;
//    }

    public List<Field> getFields(PrestoTable table) {
        return fields.get(table);
    }

    public List<Field> getFields() {
        List<Field> f = new ArrayList<>();
        fields.forEach((k, v) -> f.addAll(v));
        return f;
    }

    public void addFieldMetadata(Map<PrestoTable, List<Field>> data) {
        this.fields = data;
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
