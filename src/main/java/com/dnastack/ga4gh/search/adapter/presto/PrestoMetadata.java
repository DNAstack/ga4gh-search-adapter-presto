package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Type;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

@Builder
@AllArgsConstructor
@Slf4j
public class PrestoMetadata {

    private final PrestoAdapter presto;
    private final List<PrestoCatalog> catalogs;
    private final Map<String, PrestoTable> tables;
    private Map<PrestoTable, List<Field>> fields;

    public List<Field> getFields(PrestoTable table) {
        return fields.get(table);
    }

    Map<String, PrestoTable> getTables() {
        return this.tables;
    }

    PrestoTable getTable(String tableName) {
        PrestoTable table = tables.getOrDefault(tableName, null);
        checkArgument(table != null, format("Table %s not found", tableName));
        return table;
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

    //TODO: Verify these type mappings (via test?)
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
        } else if (prestoType.startsWith("array(varchar") || prestoType.startsWith("array(date")
                || prestoType.startsWith("array(timestamp")) { // oof
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

        log.warn("Unable to understand presto type {}, returning type STRING", prestoType);
        return Type.STRING;
    }
}
