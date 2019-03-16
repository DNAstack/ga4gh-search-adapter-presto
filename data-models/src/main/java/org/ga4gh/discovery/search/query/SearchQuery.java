/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.query;

import java.util.List;

/**
 *
 * @author mfiume
 */
public class SearchQuery extends QueryRuleSet {
    
    public SearchQuery(QueryCondition condition, List<QueryRule> rules) {
        super(condition, rules);
    }

    @Override
    public String toString() {
        return super.toString();
    }

}
