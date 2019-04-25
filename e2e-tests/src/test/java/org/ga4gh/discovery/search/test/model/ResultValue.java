package org.ga4gh.discovery.search.test.model;

import org.ga4gh.discovery.search.test.model.Field;
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
