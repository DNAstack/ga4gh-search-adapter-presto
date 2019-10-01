package com.dnastack.ga4gh.search.adapter.test.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Field {

    private String id;
    private String name;
    private Type type;
    private String[] operators;
    private String[] options;
    private String table;
}
