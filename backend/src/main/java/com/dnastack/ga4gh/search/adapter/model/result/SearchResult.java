package com.dnastack.ga4gh.search.adapter.model.result;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.dnastack.ga4gh.search.adapter.model.Field;

/** @author mfiume */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchResult {

    private List<Field> fields;
    private List<ResultRow> results;
}
