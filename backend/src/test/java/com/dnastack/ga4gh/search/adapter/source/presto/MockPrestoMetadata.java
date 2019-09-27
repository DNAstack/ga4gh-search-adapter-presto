package com.dnastack.ga4gh.search.adapter.source.presto;

import java.util.Arrays;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.dnastack.ga4gh.search.adapter.presto.PrestoField;
import com.dnastack.ga4gh.search.adapter.presto.PrestoTable;
import com.dnastack.ga4gh.search.adapter.presto.PrestoTableMetadata;

public class MockPrestoMetadata {

    public static Map<String, PrestoTableMetadata> animalsMetadata() {
        return ImmutableMap.of(
                "fact", metadata("fact",
                        "com.dnastack.pgpc.metadata",
                        "postgres",
                        "public",
                        "fact",
                        field("id", "integer"), field("name", "varchar")));
    }

    public static Map<String, PrestoTableMetadata> standardMetadata() {
        return ImmutableMap.of(
                "files",
                metadata(
                        "files",
                        "org.ga4gh.drs.objects",
                        "drs",
                        "org_ga4gh_drs",
                        "objects",
                        field("id", "varchar"),
                        field("name", "varchar"),
                        field("size", "bigint"),
                        field("created", "timestamp"),
                        field("updated", "timestamp"),
                        field("version", "varchar"),
                        field("mime_type", "varchar"),
                        field("checksums", "array(row(checksum varchar, type varchar))"),
                        field(
                                "urls",
                                "array(row(url varchar, system_metadata varchar, user_metadata varchar,authorization_metadata varchar))"),
                        field("description", "varchar"),
                        field("aliases", "array(varchar)")),
                "files_json",
                metadata("files_json",
                        "org.ga4gh.drs.json_objects",
                        "drs",
                        "org_ga4gh_drs",
                        "json_objects",
                        field("id", "varchar"), field("json", "varchar")),
                "variants",
                metadata(
                        "variants",
                        "com.google.variants",
                        "bigquery-pgc-data",
                        "pgp_variants",
                        "view_variants2_beacon",
                        field("reference_name", "varchar"),
                        field("start_position", "integer"),
                        field("end_position", "integer"),
                        field("reference_base", "varchar"),
                        field("alternate_base", "varchar"),
                        field("call_name", "varchar")),
                "facts",
                metadata(
                        "facts",
                        "com.dnastack.pgpc.metadata",
                        "postgres",
                        "public",
                        "fact",
                        field("participant_id", "varchar"),
                        field("category", "varchar"),
                        field("key", "varchar"),
                        field("raw_value", "varchar"),
                        field("numeric_value", "double")));
    }

    private static PrestoTableMetadata metadata(String name, String schema, String catalog, String prestoSchema, String prestoName, PrestoField... fields) {
        PrestoTable table = new PrestoTable(name, schema, catalog, prestoSchema, prestoName);
        return new PrestoTableMetadata(
                table, Arrays.asList(fields));
    }

    private static PrestoField field(String name, String type) {
        return new PrestoField(name, type);
    }
}
