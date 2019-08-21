package org.ga4gh.discovery.search.request;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.prestosql.sql.tree.Query;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.ga4gh.dataset.model.Schema;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchRequest {

    @JsonProperty("json_query")
    private Query jsonQuery;

    @JsonProperty("query")
    private String sqlQuery;

    private Schema expectedSchema;
}
