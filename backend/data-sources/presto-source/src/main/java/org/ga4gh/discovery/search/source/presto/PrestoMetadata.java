package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static org.ga4gh.discovery.search.source.presto.Metadata.FACTS_TABLE;
import static org.ga4gh.discovery.search.source.presto.Metadata.FILES_TABLE;
import static org.ga4gh.discovery.search.source.presto.Metadata.FILES_JSON_TABLE;
import static org.ga4gh.discovery.search.source.presto.Metadata.VARIANTS_TABLE;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PrestoMetadata {

    static Map<String, PrestoTable> PRESTO_TABLES =
            ImmutableMap.of(
                    FILES_TABLE,
                    new PrestoTable(
                            FILES_TABLE,
                            "org.ga4gh.drs.objects",
                            "drs",
                            "org_ga4gh_drs",
                            "objects"),
                    FILES_JSON_TABLE,
                    new PrestoTable(
                            FILES_JSON_TABLE,
                            "org.ga4gh.drs.json_objects",
                            "drs",
                            "org_ga4gh_drs",
                            "json_objects"),
                    VARIANTS_TABLE,
                    new PrestoTable(
                            VARIANTS_TABLE,
                            "com.google.variants",
                            "bigquery-pgc-data",
                            "pgp_variants",
                            "view_variants2_beacon"),
                    FACTS_TABLE,
                    new PrestoTable(
                            FACTS_TABLE,
                            "com.dnastack.pgpc.metadata",
                            "postgres",
                            "public",
                            "fact"));

    private final PrestoAdapter presto;
    private final Map<String, PrestoTable> prestoTables;
    PrestoMetadata(PrestoAdapter presto) {
        this.presto = presto;
        this.prestoTables = new HashMap<>();
    }

    public PrestoTable getPrestoTable(String tableName) {
        Optional<PrestoTable> table = findPrestoTable(tableName);
        checkArgument(
                table.isPresent(), String.format("Table %s is not a presto table", tableName));
        return table.get();
    }

    public Optional<PrestoTable> findPrestoTable(String tableName) {
//        return Optional.ofNullable(PRESTO_TABLES.get(tableName));
        return Optional.ofNullable(prestoTables.get(tableName));
    }

    //TODO: Fizz : Evaluate OM here
    public Map<String, PrestoTable> getPrestoTables() {
        return this.prestoTables;
    }

    public PrestoTableMetadata getTableMetadata(String tableName) {
//        PrestoTable prestoTable = PRESTO_TABLES.get(tableName);
        PrestoTable prestoTable = prestoTables.get(tableName);
        checkArgument(prestoTable != null, "Unknown table: " + tableName);
        return presto.getMetadata(prestoTable);
    }
}
