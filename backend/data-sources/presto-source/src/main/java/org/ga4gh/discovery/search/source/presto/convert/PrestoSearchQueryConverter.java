/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.source.presto.convert;

import org.ga4gh.discovery.search.query.QueryRule;
import org.ga4gh.discovery.search.query.QueryRuleSet;
import org.ga4gh.discovery.search.query.QuerySingleRule;
import org.ga4gh.discovery.search.query.SearchQuery;

/**
 *
 * @author mfiume
 */
public class PrestoSearchQueryConverter {

    public static String convertSearchQueryToPrestoSQL(QueryRule rule) {
        
        String clause = "";
        String suffix = "";
        
        if (rule instanceof SearchQuery) {
            SearchQuery query = (SearchQuery) rule;
            
            if (query.getSelect() != null) {
                clause += String.format("SELECT %s ", query.getSelect());
            }
            
            if (query.getFrom() != null) {
                clause += String.format("FROM %s ", query.getFrom());
            }
            
            clause += "WHERE ";
            
            if (query.getLimit() > 0) {
                suffix += " LIMIT " + query.getLimit();
            }
        }
        
        if (rule instanceof QuerySingleRule) {
            QuerySingleRule singleRule = (QuerySingleRule) rule;
            return String.format("%s %s \'%s\'", singleRule.getField(), singleRule.getOperator(), singleRule.getValue());
        } else if (rule instanceof QueryRuleSet) {
            QueryRuleSet ruleSet = (QueryRuleSet) rule;
            clause += "( ";
            int numRules = ruleSet.getRules().size();
            for (int i = 0; i < numRules; i++) {
                clause += convertSearchQueryToPrestoSQL(ruleSet.getRules().get(i));
                if (i < numRules - 1) {
                    clause += " " + ruleSet.getCondition().getCondition() + " ";
                }
            }
            clause += " )";
            
            return clause + suffix;
        }
        
        throw new UnsupportedOperationException("Could not convert Query Rule to SQL");
    }
    
}
