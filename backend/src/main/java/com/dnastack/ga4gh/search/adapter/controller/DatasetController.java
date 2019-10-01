package com.dnastack.ga4gh.search.adapter.controller;

import com.dnastack.ga4gh.search.adapter.model.request.DatasetRequest;
import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import javax.servlet.http.HttpServletRequest;
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

    //TODO: Update to /dataset/{id} -- Also for schemas
    @RequestMapping(value = "/api/dataset/{id}", method = RequestMethod.GET)
    public Dataset getDataset(HttpServletRequest request, @PathVariable("id") String id, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) {

        //TODO: Include pagination
        return dataSource.getDataset(new DatasetRequest(request.getRequestURL().toString(), page, pageSize, id));
    }
}
