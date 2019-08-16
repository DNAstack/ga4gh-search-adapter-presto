package org.ga4gh.discovery.search.source.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.Type;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

//@AllArgsConstructor
public class Metadata {

    public static final String PGP_CANADA = "pgp_canada";

    private static final Table PGP_CANADA_TABLE = new Table(PGP_CANADA, "com.dnastack.search.pgpcanada");

    private static final TableMetadata PGP_CANADA_METADATA =
            new TableMetadata(
                    PGP_CANADA_TABLE,
                    ImmutableList.of(
                            field("participant_id", "varchar"),
                            field("chromosome", "varchar"),
                            field("start_position", "integer"),
                            field("end_position", "integer"),
                            field("reference_base", "varchar"),
                            field("alternate_base", "varchar"),
                            field("vcf_size", "bigint"),
                            field("vcf_object", "varchar", Type.DRS_OBJECT),
                            field("category", "varchar"),
                            field("key", "varchar"),
                            field("raw_value", "varchar"),
                            field("numeric_value", "double")));

    private final Map<String, Table> tables = new HashMap<>();

    private final PrestoMetadata prestoMetadata;

    public Metadata(PrestoMetadata prestoMetadata) {
        this.prestoMetadata = prestoMetadata;
        this.prestoMetadata.getTables().forEach((qualifiedName, prestoTable) -> {
            //TODO: Qualified name or name?
            this.tables.put(qualifiedName, new Table(qualifiedName, prestoTable.getSchema()));
        });
        //TODO: HACK CITY
        this.tables.put(PGP_CANADA, PGP_CANADA_TABLE);
    }

    public List<PrestoCatalog> getCatalogs() {
        return this.prestoMetadata.getCatalogs();
    }

    public List<Field> getFields(Table table) {
        //TODO: better impl
        PrestoTable prestoTable = this.prestoMetadata.getPrestoTable(table.getName());
        List<Field> fields = toModelFields(table.getName(), this.prestoMetadata.getFields(prestoTable));
        return fields;
        //return null;
//        return this.prestoMetadata.getFields(getPrestoTable(table.getName()));
    }

    public List<Field> getFields() {
        //TODO: better impl
        List<Field> fields = new ArrayList<>();
        for (Table t : this.getTables()) {
            fields.addAll(getFields(t));
        }
        return fields;
//        return null;
//        return this.prestoMetadata.getFields();
    }

    public PrestoTable getPrestoTable(String tableName) {
        return prestoMetadata.getPrestoTable(tableName);
    }

    public boolean hasTable(String tableName) {
        return findTable(tableName).isPresent();
    }

    public List<Table> getTables() {
        return tables.values().stream().sorted().collect(toList());
    }

    public Table getTable(String tableName) {
        Table table = tables.getOrDefault(tableName, null);
        checkArgument(table != null, format("Table %s not found", tableName));
        return table;
    }

    public Optional<Table> findTable(String tableName) {
        return tables.values().stream().filter(t -> t.getName().equals(tableName)).findAny();
    }

    public TableMetadata getTableMetadata(String tableName) {
        Table table = tables.get(tableName);
        Preconditions.checkArgument(
                table != null, String.format("Table %s doesn't exist", tableName));
        if (PGP_CANADA.equals(tableName)) {
            return PGP_CANADA_METADATA;
        }
        return toModelMetadata(table, prestoMetadata.getTableMetadata(tableName));
    }

    public TableMetadata toModelMetadata(Table table, PrestoTableMetadata prestoTableMetadata) {
        return new TableMetadata(
                table, toModelFields(table.getName(), prestoTableMetadata.getFields()));
    }

    public List<Field> toModelFields(String tableName, List<PrestoField> prestoFields) {
        return prestoFields.stream().map(f -> toModelField(tableName, f)).collect(toList());
    }

    private static Field toModelField(String tableName, PrestoField prestoField) {
        return toModelField(tableName, prestoField, null);
    }

    private static Field toModelField(String tableName, PrestoField prestoField, Type modelType) {
        Type type = modelType == null ? prestoToPrimitiveType(prestoField.getType()) : modelType;
        return new Field(
                tableName + "." + prestoField.getName(),
                prestoField.getName(),
                type,
                operatorsForType(type),
                null,
                tableName);
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

    static Set<String> jdbcTypesFor(Type type) {
        switch (type) {
            case BOOLEAN:
                return ImmutableSet.of("boolean");
            case DATE:
                return ImmutableSet.of("timestamp");
            case DRS_OBJECT:
                return ImmutableSet.of("varchar", "json");
            case JSON:
                return ImmutableSet.of("array", "json", "row");
            case NUMBER:
                return ImmutableSet.of("integer", "double", "bigint");
            case STRING:
                return ImmutableSet.of("varchar");
                //TODO: is this correct??
            case NUMBER_ARRAY:
            case STRING_ARRAY:
                return ImmutableSet.of("array");
            default:
                throw new IllegalStateException("Unknown type: " + type);
        }
    }

    static Type prestoToPrimitiveType(String prestoType) {
        if (prestoType.equals("integer")
                || prestoType.equals("double")
                || prestoType.equals("bigint")) {
            return Type.NUMBER;
        } else if (prestoType.equals("timestamp")) {
            return Type.DATE;
            //TODO: Is this accurate? Need to special case?
        } else if (prestoType.equals("timestamp with time zone")) {
            return Type.DATE;
            //TODO: This or the above is definitely wrong
        } else if (prestoType.equals("date")) {
            return Type.DATE;
        } else if (prestoType.startsWith("boolean")) {
            return Type.BOOLEAN;
        } else if (prestoType.startsWith("varchar")) {
            return Type.STRING;
        } else if (prestoType.startsWith("array(varchar")) {
            return Type.STRING_ARRAY;
        } else if (prestoType.startsWith("array(row") || prestoType.startsWith("json")) {
            return Type.JSON;
        } else if (prestoType.startsWith("array(double")
                    || prestoType.startsWith("array(int") || prestoType.equals(Type.NUMBER_ARRAY.toString())) {
            return Type.NUMBER_ARRAY;
            // TODO: Double check correctness of below, was a best guess
        } else if (prestoType.startsWith("row(")) {
            return Type.JSON;
        }
        throw new RuntimeException("Unknown mapping for Presto field type " + prestoType);
    }

    //TODO: delete me
    private static Field field(String name, String type) {
        return toModelField(PGP_CANADA, new PrestoField(name, type));
    }

    private static Field field(String name, String type, Type modelType) {
        return toModelField(PGP_CANADA, new PrestoField(name, type), modelType);
    }
}
