package com.dnastack.ga4gh.search.adapter.model.result;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.dnastack.ga4gh.search.adapter.model.Field;

/** @author mfiume */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultValue {

    private Field field;
    private Object value;
}
