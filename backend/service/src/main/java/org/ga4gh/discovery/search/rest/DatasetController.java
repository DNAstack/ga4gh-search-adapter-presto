package org.ga4gh.discovery.search.rest;

import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.result.SearchResult;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class DatasetController {
    @Autowired
    SearchSource dataSource;

    @RequestMapping(value = "/api/datasets", method = RequestMethod.GET)
    public ListDatasetsResponse getDatasets() {
        return dataSource.getDatasets();
    }

    //TODO: Update to /dataset/{id} -- Also for schemas
    @RequestMapping(value = "/api/datasets/{id}", method = RequestMethod.GET)
    public Dataset getDataset(@PathVariable("id") String id) {
        //TODO: Include pagination
        return dataSource.getDataset(id);
    }
}
