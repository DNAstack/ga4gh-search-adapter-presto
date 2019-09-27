package com.dnastack.ga4gh.search.adapter.presto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class PrestoField {

    private final String name;
    private final String type;
}
