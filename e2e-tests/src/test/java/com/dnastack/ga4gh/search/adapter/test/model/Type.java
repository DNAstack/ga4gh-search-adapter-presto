package com.dnastack.ga4gh.search.adapter.test.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Type {
    BOOLEAN {
        @Override
        public String toString() {
            return "boolean";
        }
    },
    NUMBER {
        @Override
        public String toString() {
            return "number";
        }
    },
    STRING {
        @Override
        public String toString() {
            return "string";
        }
    },
    STRING_ARRAY {
        @Override
        public String toString() {
            return "string[]";
        }
    },
    JSON {
        @Override
        public String toString() {
            return "json";
        }
    },
    DATE {
        @Override
        public String toString() {
            return "date";
        }
    },
    DRS_OBJECT {
        @Override
        public String toString() {
            return "org.ga4gh.drs";
        }
    };

    @JsonValue
    @Override
    public String toString() {
        return super.toString();
    }
}
