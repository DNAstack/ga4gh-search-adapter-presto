package com.dnastack.ga4gh.search.adapter.presto;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PrestoSchema {
    private String name;

    public String getName() {
        return name;
    }
}
