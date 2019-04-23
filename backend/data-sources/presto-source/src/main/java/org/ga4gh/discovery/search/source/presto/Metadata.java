package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Metadata {

    public static final String FILES_TABLE = "files";
    public static final String FILES_JSON_TABLE = "files_json";
    public static final String VARIANTS_TABLE = "variants";
    public static final String FACTS_TABLE = "facts";
    public static final String DEMO_VIEW = "demo_view";

    private static final Map<String, Table> TABLES =
            ImmutableMap.of(
                    FILES_TABLE,
                    new Table(FILES_TABLE, "org.ga4gh.drs.objects"),
                    FILES_JSON_TABLE,
                    new Table(FILES_JSON_TABLE, "org.ga4gh.drs.json_objects"),
                    VARIANTS_TABLE,
                    new Table(VARIANTS_TABLE, "com.google.variants"),
                    FACTS_TABLE,
                    new Table(FACTS_TABLE, "com.dnastack.pgpc.metadata"),
                    DEMO_VIEW,
                    new Table(DEMO_VIEW, "com.dnastack.search.demo.view"));

    private static final TableMetadata DEMO_VIEW_METADATA =
            new TableMetadata(
                    TABLES.get(DEMO_VIEW),
                    ImmutableList.of(
                            field("participant_id", "varchar"),
                            field("chromosome", "varchar"),
                            field("start_position", "integer"),
                            field("end_position", "integer"),
                            field("reference_base", "varchar"),
                            field("alternate_base", "varchar"),
                            field("vcf_size", "bigint"),
                            field("vcf_object", "varchar"),
                            field("category", "varchar"),
                            field("key", "varchar"),
                            field("raw_value", "varchar"),
                            field("numeric_value", "double")));

    private final PrestoMetadata prestoMetadata;

    public PrestoTable getPrestoTable(String tableName) {
        return prestoMetadata.getPrestoTable(tableName);
    }

    public boolean hasTable(String tableName) {
        return findTable(tableName).isPresent();
    }

    public List<Table> getTables() {
        return TABLES.values().stream().sorted().collect(toList());
    }

    public Table getTable(String tableName) {
        Optional<Table> table = findTable(tableName);
        checkArgument(table.isPresent(), format("Table %s not found", tableName));
        return table.get();
    }

    public Optional<Table> findTable(String tableName) {
        return TABLES.values().stream().filter(t -> t.getName().equals(tableName)).findAny();
    }

    public TableMetadata getTableMetadata(String tableName) {
        Table table = TABLES.get(tableName);
        Preconditions.checkArgument(
                table != null, String.format("Table %s doesn't exist", tableName));
        if (DEMO_VIEW.equals(tableName)) {
            return DEMO_VIEW_METADATA;
        } else {
            return toModelMetadata(table, prestoMetadata.getTableMetadata(tableName));
        }
    }

    public TableMetadata toModelMetadata(Table table, PrestoTableMetadata prestoTableMetadata) {
        return new TableMetadata(
                table, toModelFields(table.getName(), prestoTableMetadata.getFields()));
    }

    public List<Field> toModelFields(String tableName, List<PrestoField> prestoFields) {
        return prestoFields.stream().map(f -> toModelField(tableName, f)).collect(toList());
    }

    private static Field toModelField(String tableName, PrestoField prestoField) {
        Type type = prestoToPrimativeType(prestoField.getType());
        return new Field(
                tableName + "." + prestoField.getName(),
                prestoField.getName(),
                type,
                operatorsForType(type),
                null,
                tableName);
    }

    private static Field field(String name, String type) {
        return toModelField(DEMO_VIEW, new PrestoField(name, type));
    }

    static String[] operatorsForType(Type type) {
        switch (type) {
            case NUMBER:
                return new String[] {"=", "!=", "<", "<=", ">", ">="};
            case STRING:
                return new String[] {"=", "!=", "contains", "like"};
            case DATE:
                return new String[] {"=", "!=", "<", "<=", ">", ">="};
            case STRING_ARRAY:
                return new String[] {"all", "in", "none"};
            case BOOLEAN:
                return new String[] {};
            case JSON:
                return new String[] {"=", "!="};
            default:
                return new String[0];
        }
    }

    static Type prestoToPrimativeType(String prestoType) {
        if (prestoType.equals("integer")
                || prestoType.equals("double")
                || prestoType.equals("bigint")) {
            return Type.NUMBER;
        } else if (prestoType.equals("timestamp")) {
            return Type.DATE;
        } else if (prestoType.startsWith("boolean")) {
            return Type.BOOLEAN;
        } else if (prestoType.startsWith("varchar")) {
            return Type.STRING;
        } else if (prestoType.startsWith("array(varchar")) {
            return Type.STRING_ARRAY;
        } else if (prestoType.startsWith("array(row") || prestoType.startsWith("json")) {
            return Type.JSON;
        }
        throw new RuntimeException("Unknown mapping for Presto field type " + prestoType);
    }
}
