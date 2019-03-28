package org.ga4gh.discovery.search;

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
    }
}
