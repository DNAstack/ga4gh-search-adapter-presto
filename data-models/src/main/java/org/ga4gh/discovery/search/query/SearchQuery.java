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
    
    private String select;
    private String from;
    private int limit;
    
     public SearchQuery(QueryCondition condition, List<QueryRule> rules) {
        super(condition, rules);
        select = "*";
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getSelect() {
        return select;
    }

    public void setSelect(String select) {
        this.select = select;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
    
    
    @Override
    public String toString() {
        return "SearchQuery{" + "seelect=" + select + "from=" + from + " " + super.toString() + "limit=" + limit + '}';
    }

}
