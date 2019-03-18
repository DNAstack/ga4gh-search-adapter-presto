/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.rest.serialize;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import org.ga4gh.discovery.search.query.QueryRuleSet;
import org.ga4gh.discovery.search.query.SearchQuery;

/**
 *
 * @author mfiume
 */
public class SearchQueryDeserializer extends JsonDeserializer<SearchQuery> {

    @Override
    public SearchQuery deserialize(JsonParser jp, DeserializationContext dc) throws IOException, JsonProcessingException {
        QueryRuleSet rootQueryRuleSet = (QueryRuleSet) new QueryRuleDeserializer().deserialize(jp, dc);
        return new SearchQuery(rootQueryRuleSet.getCondition(), rootQueryRuleSet.getRules());
    }
    
}
