package com.dnastack.ga4gh.search.tables;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TableData {

    @JsonProperty("data_model")
    private Map<String, Object> dataModel;

    @JsonProperty("data")
    private List<Map<String, Object>> data;

    @JsonProperty("errors")
    private List<TableError> errors;

    @JsonProperty("pagination")
    private Pagination pagination;

}
