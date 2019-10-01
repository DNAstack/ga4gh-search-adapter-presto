package com.dnastack.ga4gh.search.adapter.test.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** @author mfiume */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchResult {

    private List<Field> fields;
    private List<ResultRow> results;
}
