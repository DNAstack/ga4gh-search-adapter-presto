package org.ga4gh.discovery.search.test.model;

import java.util.List;
import org.ga4gh.discovery.search.test.model.Field;
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
