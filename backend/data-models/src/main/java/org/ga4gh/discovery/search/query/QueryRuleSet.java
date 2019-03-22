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
public class QueryRuleSet extends QueryRule {
    
    private QueryCondition condition;
    private List<QueryRule> rules;
    
    public QueryRuleSet() {
    }

    public QueryRuleSet(QueryCondition condition, List<QueryRule> rules) {
        this.condition = condition;
        this.rules = rules;
    }

    public QueryCondition getCondition() {
        return condition;
    }

    public List<QueryRule> getRules() {
        return rules;
    }

    public void setCondition(QueryCondition condition) {
        this.condition = condition;
    }

    public void setRules(List<QueryRule> rules) {
        this.rules = rules;
    }

    @Override
    public String toString() {
        return "QueryRuleSet{" + "condition=" + condition + ", rules=" + rules + '}';
    }
    
}
