package org.ga4gh.discovery.search.rest;

import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DatasetController {
    @Autowired
    SearchSource dataSource;

    //TODO (fizz): BADDDDDD, but is there a better way?
    @Autowired
    DatasetApiService datasetApiService;

    @RequestMapping(value = "/api/datasets", method = RequestMethod.GET)
    public ListDatasetsResponse getSchemas() {
        return datasetApiService.listDatasets();
        //return datasetApiService.listSchemas();
    }

    //TODO: Update to /dataset/{id} -- Also for schemas
    @RequestMapping(value = "/api/datasets/{id}", method = RequestMethod.GET)
    public Dataset getSchema(@PathVariable("id") String id) {
        //TODO: Include pagination
        return datasetApiService.getDatasetResponse(id, null);
    }
}
