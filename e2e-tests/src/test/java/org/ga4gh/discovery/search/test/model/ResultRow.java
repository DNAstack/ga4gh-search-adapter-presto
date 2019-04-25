package org.ga4gh.discovery.search.test.model;

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
