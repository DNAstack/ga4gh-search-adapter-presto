package com.dnastack.ga4gh.search.tables;

import com.dnastack.ga4gh.search.adapter.shared.SearchAuthRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class ListTables {

    @JsonProperty("tables")
    private List<TableInfo> tableInfos;

    @JsonProperty("errors")
    private List<TableError> errors;

    @JsonProperty("pagination")
    private Pagination pagination;

}
