package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Type;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

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

    static Type prestoToPrimitiveType(String prestoType) {
        boolean isArray = prestoType.startsWith("array(");
        String prestoTypeConcat = (isArray) ? prestoType.substring(prestoType.indexOf('(') + 1) : prestoType;

        if (prestoTypeConcat.startsWith("int") || prestoTypeConcat.startsWith("tinyint") ||
                prestoTypeConcat.startsWith("smallint") || prestoTypeConcat.startsWith("bigint") ||
                prestoTypeConcat.startsWith("double") || prestoTypeConcat.startsWith("real")) {
            return (isArray) ? Type.NUMBER_ARRAY : Type.NUMBER;
        } else if (prestoTypeConcat.startsWith(Type.NUMBER_ARRAY.toString())) {
            return Type.NUMBER_ARRAY;
        } else if (prestoTypeConcat.startsWith("timestamp") || prestoTypeConcat.startsWith("timestamp with time zone") ||
                prestoTypeConcat.startsWith("date") || prestoTypeConcat.startsWith("time")) {
            return (isArray) ? Type.DATETIME_ARRAY : Type.DATETIME;
        } else if (prestoTypeConcat.startsWith("varchar") || prestoTypeConcat.startsWith("char")) {
            return (isArray) ? Type.STRING_ARRAY : Type.STRING;
        } else if (prestoTypeConcat.startsWith("row(")) {
            return (isArray) ? Type.ARRAY_ROW : Type.ROW;
        } else if (prestoTypeConcat.startsWith("boolean")) {
            return Type.BOOLEAN;
        } else if (prestoTypeConcat.startsWith("varbinary")) {
            return Type.VARBINARY;
        } else if (prestoTypeConcat.startsWith("json")) {
            return Type.JSON;
        }

        log.warn("Unable to understand presto type {}, returning type STRING", prestoType);
        return Type.STRING;
    }
}
