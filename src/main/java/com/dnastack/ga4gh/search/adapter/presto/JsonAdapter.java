package com.dnastack.ga4gh.search.adapter.presto;

import java.util.List;

public class JsonAdapter {
    static List<String> intAliases = List.of("int","timestamp");
    static List<String> numberAliases = List.of("number","float","double","real");
    static List<String> booleanAliases = List.of("bool");

    static boolean isArray(String prestoType) {
        return prestoType.contains("[]") || prestoType.contains("array");
    }

    static String toJsonType(String prestoType) {
        if (prestoType == null) {
            return "NULL";
        }

        String type = prestoType.toLowerCase();
        if (type.equals("null")) { //todo: improve null logic
            return "NULL";
        }
        if (intAliases.stream().anyMatch(type::contains)) {
            return "int";
        }
        if (numberAliases.stream().anyMatch(type::contains)) {
            return "number";
        }
        if (booleanAliases.stream().anyMatch(type::contains)) {
            return "boolean";
        }

        return "string";
    }
}
