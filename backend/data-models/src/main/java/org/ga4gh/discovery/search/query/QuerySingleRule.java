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
public class QuerySingleRule extends QueryRule {
    
    private String field;
    private String operator;
    private String value;

    public QuerySingleRule() {}
    
    public QuerySingleRule(String field, String operator, String value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public String getOperator() {
        return operator;
    }

    public String getValue() {
        return value;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "QuerySingleRule{" + "field=" + field + ", operator=" + operator + ", value=" + value + '}';
    }
    
}
