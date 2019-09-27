package com.dnastack.ga4gh.search.adapter.controller;

import java.util.List;
import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FieldsController {

    @Autowired
    SearchSource dataSource;

    @RequestMapping("/api/fields")
    public List<Field> fields(@RequestParam(value = "", required = false) String table) {
        return dataSource.getFields(table);
    }
}
