package com.dnastack.ga4gh.search.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TablesList {

    @JsonProperty("tables")
    private List<TableInfo> tableInfos;

    // TODO: this is only a list for historical reasons.
    // remove this after co-ordinating with frontend.
    @JsonProperty("errors")
    @Deprecated
    private List<TableError> errors;

    @JsonProperty("error")
    private TableError error;

    @JsonProperty("pagination")
    private Pagination pagination;

    @JsonProperty("index")
    private List<PageIndexEntry> index;

    public TablesList(List<TableInfo> tableInfos, TableError error, Pagination pagination) {
        this.tableInfos = tableInfos;
        if (error != null) {
            this.errors = List.of(error);
        }
        this.error = error;
        this.pagination = pagination;
    }


}
