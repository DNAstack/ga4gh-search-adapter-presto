package org.ga4gh.discovery.search.source.presto;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/** Simulates presto responses */
public class MockPrestoAdapter implements PrestoAdapter {

    private static final Map<String, PrestoTableMetadata> METADATA =
            ImmutableMap.of(
                    "files",
                            metadata(
                                    "files",
                                    field("id", "varchar"),
                                    field("name", "varchar"),
                                    field("size", "bigint"),
                                    field("created", "timestamp"),
                                    field("updated", "timestamp"),
                                    field("version", "varchar"),
                                    field("mime_type", "varchar"),
                                    field(
                                            "checksums",
                                            "array(row(checksum varchar, type varchar))"),
                                    field(
                                            "urls",
                                            "array(row(url varchar, system_metadata varchar, user_metadata varchar,authorization_metadata varchar))"),
                                    field("description", "varchar"),
                                    field("aliases", "array(varchar)")),
                    "variants",
                            metadata(
                                    "variants",
                                    field("reference_name", "varchar"),
                                    field("start_position", "integer"),
                                    field("end_position", "integer"),
                                    field("reference_base", "varchar"),
                                    field("alternate_base", "varchar"),
                                    field("call_name", "varchar")),
                    "facts",
                            metadata(
                                    "facts",
                                    field("participant_id", "varchar"),
                                    field("category", "varchar"),
                                    field("key", "varchar"),
                                    field("raw_value", "varchar"),
                                    field("numeric_value", "double")));

    private static PrestoTableMetadata metadata(String table, PrestoField... fields) {
        return new PrestoTableMetadata(
                PrestoMetadata.PRESTO_TABLES.get(table), Arrays.asList(fields));
    }

    private static PrestoField field(String name, String type) {
        return new PrestoField(name, type);
    }

    @Override
    public PrestoTableMetadata getMetadata(PrestoTable table) {
        PrestoTableMetadata metadata = METADATA.get(table.getName());
        Preconditions.checkArgument(
                metadata != null, "table " + table.getName() + " doesn't exist");
        return metadata;
    }

    @Override
    public void query(String prestoSQL, Consumer<ResultSet> resultProcessor) {
        throw new UnsupportedOperationException();
    }
}
