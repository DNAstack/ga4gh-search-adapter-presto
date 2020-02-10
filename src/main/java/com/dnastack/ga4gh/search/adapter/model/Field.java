package com.dnastack.ga4gh.search.adapter.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Field {
    private String id;
    private String name;
    private Type type;
    private String[] options;
    private String table;
}
