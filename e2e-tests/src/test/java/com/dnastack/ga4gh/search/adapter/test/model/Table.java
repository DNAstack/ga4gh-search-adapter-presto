package com.dnastack.ga4gh.search.adapter.test.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class Table {
    private String name;
    private String description;
    private Map<String, Object> dataModel;
    private List<Map<String, Object>> data;
    private Pagination pagination;

    // track additional JSON properties so we can see them when we log errors

    @JsonAnySetter
    private Map<String, Object> additionalProperties = new LinkedHashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }
}
