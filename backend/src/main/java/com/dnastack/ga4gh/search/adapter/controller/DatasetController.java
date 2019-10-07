package com.dnastack.ga4gh.search.adapter.controller;

import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import lombok.extern.slf4j.Slf4j;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class DatasetController {

    @Autowired
    SearchSource dataSource;

    @RequestMapping(value = "/api/datasets", method = RequestMethod.GET)
    public ListDatasetsResponse getDatasets() {
        return dataSource.getDatasets();
    }

    @RequestMapping(value = "/api/dataset/{id}", method = RequestMethod.GET)
    public Dataset getDataset(@PathVariable("id") String id, @RequestParam(required = false) Integer pageSize) {
        return dataSource.getDataset(id, pageSize);
    }
}
