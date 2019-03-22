/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ga4gh.discovery.search.result;

import org.ga4gh.discovery.search.Field;

/**
 *
 * @author mfiume
 */
public class ResultValue {
    
    private final Field field;
    private final Object value;

    public ResultValue(Field field, Object value) {
        this.field = field;
        this.value = value;
    }

    public Field getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }
    
}
