package com.dnastack.ga4gh.search.adapter.test.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.net.URI;

@Data
public class PageIndexEntry {
    @JsonProperty("description")
    private String description;

    @JsonProperty("url")
    private URI url;

    @JsonProperty("page")
    private int page;
}

