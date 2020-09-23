package com.dnastack.ga4gh.search.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo implements Comparable<TableInfo> {

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;


    //private Map<String, Object> dataModel;
    @JsonProperty("data_model")
    private DataModel dataModel;

    @Override
    public int compareTo(TableInfo o) {
        return this.name.compareTo(o.name);
    }

}
