package org.ga4gh.discovery.search.source.presto;

import lombok.AllArgsConstructor;

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
//
//    public PrestoMetadata(PrestoAdapter presto, Map<String, PrestoTable> tables) {
//        this.tables = tables;
//        this.presto = presto;
//    }

    public PrestoTable getPrestoTable(String tableName) {
        Optional<PrestoTable> table = findPrestoTable(tableName);
        checkArgument(
                table.isPresent(), String.format("Table %s is not a presto table", tableName));
        return table.get();
    }

    public Optional<PrestoTable> findPrestoTable(String tableName) {
//        return Optional.ofNullable(PRESTO_TABLES.get(tableName));
        return Optional.ofNullable(tables.get(tableName));
    }

    //TODO: Fizz : Evaluate OM here
    public Map<String, PrestoTable> getTables() {
        return this.tables;
    }

    public PrestoTableMetadata getTableMetadata(String tableName) {
//        PrestoTable prestoTable = PRESTO_TABLES.get(tableName);
        PrestoTable prestoTable = tables.get(tableName);
        checkArgument(prestoTable != null, "Unknown table: " + tableName);
        return presto.getMetadata(prestoTable);
    }
}
