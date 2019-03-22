/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.query;

/**
 *
 * @author mfiume
 */
public class QueryCondition {
    
    private String condition;
    
    public QueryCondition() {}
    
    public QueryCondition(String condition) {
        this.condition = condition;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    @Override
    public String toString() {
        return "QueryCondition{" + "condition=" + condition + '}';
    }
    
}
