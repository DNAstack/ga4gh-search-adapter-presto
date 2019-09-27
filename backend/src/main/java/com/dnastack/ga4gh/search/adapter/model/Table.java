package com.dnastack.ga4gh.search.adapter.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Table implements Comparable<Table> {

    private final String name;
    private final String schema;

    @Override
    public int compareTo(Table o) {
        return this.name.compareTo(o.name);
    }
}
