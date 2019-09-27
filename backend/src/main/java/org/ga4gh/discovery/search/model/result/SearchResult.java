package org.ga4gh.discovery.search.model.result;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.ga4gh.discovery.search.model.Field;

/** @author mfiume */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchResult {

    private List<Field> fields;
    private List<ResultRow> results;
}
