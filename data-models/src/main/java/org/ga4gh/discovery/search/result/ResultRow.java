/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.result;

import java.util.List;

/**
 *
 * @author mfiume
 */
public class ResultRow {
    
    private List<ResultValue> values;
    
    public ResultRow() {}

    public ResultRow(List<ResultValue> values) {
        this.values = values;
    }

    public List<ResultValue> getValues() {
        return values;
    }

    public void setValues(List<ResultValue> values) {
        this.values = values;
    }
 
    
}
