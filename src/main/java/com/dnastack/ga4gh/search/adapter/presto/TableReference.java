package com.dnastack.ga4gh.search.adapter.presto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class TableReference implements Comparable<TableReference> {

    private final String name;
    private final String schema;

    @Override
    public int compareTo(TableReference o) {
        return this.name.compareTo(o.name);
    }
}
