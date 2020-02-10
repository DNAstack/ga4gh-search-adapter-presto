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
    private static Map<String, Type> prestoToTypeMap = initPrestoToTypeMap();

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
        String prestoTypeModified = prestoType;
        if (prestoType.startsWith("array(row")) {
            prestoTypeModified = "array(row";
        } else if (!prestoType.startsWith("array") && prestoType.contains("(")) {
            prestoTypeModified = prestoType.substring(0, prestoType.indexOf('('));
        }
        Type t  = prestoToTypeMap.getOrDefault(prestoTypeModified , null);
        if (t != null) {
            return t;
        }
        log.warn("Unable to understand presto type {}, returning type STRING", prestoType);
        return Type.STRING;

    }

    private static Map<String, Type> initPrestoToTypeMap() {
        Map<String, Type> prestoToTypeMap = new HashMap<>();

        prestoToTypeMap.put("int", Type.NUMBER);
        prestoToTypeMap.put("integer", Type.NUMBER);
        prestoToTypeMap.put("tinyint", Type.NUMBER);
        prestoToTypeMap.put("smallint", Type.NUMBER);
        prestoToTypeMap.put("bigint", Type.NUMBER);
        prestoToTypeMap.put("double", Type.NUMBER);
        prestoToTypeMap.put("real", Type.NUMBER);
        prestoToTypeMap.put("array(int)", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(tinyint)", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(smallint)", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(bigint)", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(double)", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(real)", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("number[]", Type.NUMBER_ARRAY);

        prestoToTypeMap.put("timestamp", Type.DATETIME);
        prestoToTypeMap.put("timestamp with time zone", Type.DATETIME);
        prestoToTypeMap.put("date", Type.DATETIME);
        prestoToTypeMap.put("time", Type.DATETIME);
        prestoToTypeMap.put("array(timestamp)", Type.DATETIME_ARRAY);
        prestoToTypeMap.put("array(timestamp with time zone)", Type.DATETIME_ARRAY);
        prestoToTypeMap.put("array(date)", Type.DATETIME_ARRAY);
        prestoToTypeMap.put("array(time)", Type.DATETIME_ARRAY);

        prestoToTypeMap.put("varchar", Type.STRING);
        prestoToTypeMap.put("char", Type.STRING);
        prestoToTypeMap.put("array(varchar)", Type.STRING_ARRAY);
        prestoToTypeMap.put("array(char)", Type.STRING_ARRAY);

        prestoToTypeMap.put("row", Type.ROW);
        prestoToTypeMap.put("array(row", Type.ROW_ARRAY);

        prestoToTypeMap.put("boolean", Type.BOOLEAN);
        prestoToTypeMap.put("varbinary", Type.VARBINARY);
        prestoToTypeMap.put("json", Type.JSON);

        return prestoToTypeMap;
    }
}
