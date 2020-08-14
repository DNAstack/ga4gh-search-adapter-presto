package com.dnastack.ga4gh.search.adapter.test.model;

import java.util.List;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

@Data
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ListTableResponse {
    private List<Table> tables;
    private Pagination pagination;
    private List<PageIndexEntry> index;
}
