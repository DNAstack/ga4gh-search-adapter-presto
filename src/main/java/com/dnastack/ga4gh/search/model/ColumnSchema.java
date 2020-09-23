package com.dnastack.ga4gh.search.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class ColumnSchema {
    @JsonProperty("items")
    private ColumnSchema items;

    @JsonProperty("format")
    private String format;

    @JsonProperty("type")
    private String type;

    @JsonProperty("$comment")
    private String comment;

    @JsonProperty("$ref")
    private String ref;
}
