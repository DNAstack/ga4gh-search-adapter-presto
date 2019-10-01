package com.dnastack.ga4gh.search.adapter.test.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** @author mfiume */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultValue {

    private Field field;
    private Object value;
}
