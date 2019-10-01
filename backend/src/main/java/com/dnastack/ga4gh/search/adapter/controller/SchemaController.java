package com.dnastack.ga4gh.search.adapter.controller;

import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Schema;
import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SchemaController {
    @Autowired
    SearchSource dataSource;

    @RequestMapping(value = "/api/schemas", method = RequestMethod.GET)
    public ListSchemasResponse getSchemas() {
        return dataSource.getSchemas();
    }

    @RequestMapping(value = "/api/schema/{id}", method = RequestMethod.GET)
    public Schema getSchema(@PathVariable("id") String id) {
        return dataSource.getSchema(id);
    }
}
