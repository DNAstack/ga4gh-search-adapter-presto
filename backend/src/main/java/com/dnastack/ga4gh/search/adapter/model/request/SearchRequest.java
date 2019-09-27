package com.dnastack.ga4gh.search.adapter.model.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.sql.tree.Query;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchRequest {

    @JsonProperty("json_query")
    private Query jsonQuery;

    @JsonProperty("query")
    private String sqlQuery;

    private Map<String, Object> expectedSchema;
}
