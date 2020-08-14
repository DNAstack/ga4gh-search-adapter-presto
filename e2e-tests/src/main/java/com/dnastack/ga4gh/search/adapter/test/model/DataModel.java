package com.dnastack.ga4gh.search.adapter.test.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.net.URI;
import java.util.Map;

@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
public class DataModel {
    @JsonProperty("$id")
    private URI id;

    @JsonProperty("description")
    private String description;

    @JsonProperty("$schema")
    private URI schema;

    @JsonProperty("properties")
    private Map<String, ColumnSchema> properties;

    @JsonProperty("$ref")
    private String ref;
}
