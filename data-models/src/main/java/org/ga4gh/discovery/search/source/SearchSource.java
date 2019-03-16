/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.source;

import java.util.List;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.result.SearchResult;

/**
 * 
 * @author mfiume
 */
public interface SearchSource {

    public List<Field> getFields();

    public SearchResult search(SearchQuery query);
    
}
