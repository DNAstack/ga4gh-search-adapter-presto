package org.ga4gh.discovery.search.test.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.prestosql.sql.tree.Query;
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
}
