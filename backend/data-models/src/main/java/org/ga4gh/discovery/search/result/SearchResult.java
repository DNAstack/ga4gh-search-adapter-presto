/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.result;

import java.util.List;
import org.ga4gh.discovery.search.Field;

/**
 *
 * @author mfiume
 */
public class SearchResult {
    
    private final List<Field> fields;
    private final List<ResultRow> results;

    public SearchResult(List<Field> fields, List<ResultRow> results) {
        this.fields = fields;
        this.results = results;
    }

    public List<Field> getFields() {
        return fields;
    }

    public List<ResultRow> getResults() {
        return results;
    }
    
}
