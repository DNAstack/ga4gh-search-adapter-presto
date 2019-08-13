package org.ga4gh.discovery.search.rest;

import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Schema;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SchemaController {
    @Autowired
    SearchSource dataSource;

    //TODO (fizz): BADDDDDD, but is there a better way?
//    @Autowired
//    DatasetApiService datasetApiService;

    @RequestMapping(value = "/api/schemas", method = RequestMethod.GET)
    public ListSchemasResponse getSchemas() {
        //return datasetApiService.listSchemas();
        return dataSource.getSchemas();
    }

    @RequestMapping(value = "/api/schemas/{id}", method = RequestMethod.GET)
    public Schema getSchema(@PathVariable("id") String id) {
        //return datasetApiService.getSchema(id);
        return dataSource.getSchema(id);
    }
}
