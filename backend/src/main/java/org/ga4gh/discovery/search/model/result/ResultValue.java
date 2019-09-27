package org.ga4gh.discovery.search.model.result;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.ga4gh.discovery.search.model.Field;

/** @author mfiume */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultValue {

    private Field field;
    private Object value;
}
