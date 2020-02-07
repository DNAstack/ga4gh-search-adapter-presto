package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Type;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
    private static TreeMap<String, Type> prestoToTypeMap;

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
        if (prestoToTypeMap == null) {
            prestoToTypeMap = new TreeMap<>();
            initPrestoToTypeMap();
        }
        for (Map.Entry<String, Type> entry : prestoToTypeMap.entrySet()) {
            if (prestoType.startsWith(entry.getKey())) {
                return entry.getValue();
            }
        }
        log.warn("Unable to understand presto type {}, returning type STRING", prestoType);
        return Type.STRING;

    }

    private static void initPrestoToTypeMap() {
        prestoToTypeMap.put("int", Type.NUMBER);
        prestoToTypeMap.put("tinyint", Type.NUMBER);
        prestoToTypeMap.put("smallint", Type.NUMBER);
        prestoToTypeMap.put("bigint", Type.NUMBER);
        prestoToTypeMap.put("double", Type.NUMBER);
        prestoToTypeMap.put("real", Type.NUMBER);
        prestoToTypeMap.put("array(int", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(tinyint", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(smallint", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(bigint", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(double", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("array(real", Type.NUMBER_ARRAY);
        prestoToTypeMap.put("number[]", Type.NUMBER_ARRAY);

        prestoToTypeMap.put("timestamp", Type.DATETIME);
        prestoToTypeMap.put("timestamp with time zone", Type.DATETIME);
        prestoToTypeMap.put("date", Type.DATETIME);
        prestoToTypeMap.put("time", Type.DATETIME);
        prestoToTypeMap.put("array(timestamp", Type.DATETIME_ARRAY);
        prestoToTypeMap.put("array(timestamp with time zone", Type.DATETIME_ARRAY);
        prestoToTypeMap.put("array(date", Type.DATETIME_ARRAY);
        prestoToTypeMap.put("array(time", Type.DATETIME_ARRAY);

        prestoToTypeMap.put("varchar", Type.STRING);
        prestoToTypeMap.put("char", Type.STRING);
        prestoToTypeMap.put("array(varchar", Type.STRING_ARRAY);
        prestoToTypeMap.put("array(char", Type.STRING_ARRAY);

        prestoToTypeMap.put("row", Type.ROW);
        prestoToTypeMap.put("array(row", Type.ROW_ARRAY);

        prestoToTypeMap.put("boolean", Type.BOOLEAN);
        prestoToTypeMap.put("varbinary", Type.VARBINARY);
        prestoToTypeMap.put("json", Type.JSON);
    }
}
