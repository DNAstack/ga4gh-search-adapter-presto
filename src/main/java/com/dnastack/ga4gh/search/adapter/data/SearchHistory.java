package com.dnastack.ga4gh.search.adapter.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.ZonedDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchHistory {

    @JsonIgnore
    private String userId;

    @JsonProperty("submission_date")
    private ZonedDateTime submissionDate;

    @JsonProperty("query")
    private String sqlQuery;

    @JsonProperty
    private Boolean succeeded;
}
