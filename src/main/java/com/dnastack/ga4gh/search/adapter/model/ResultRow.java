package com.dnastack.ga4gh.search.adapter.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** @author mfiume */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultRow {

    private List<ResultValue> values;
}
