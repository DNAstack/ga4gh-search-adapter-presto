package com.dnastack.ga4gh.search.adapter.controller;

import org.ga4gh.dataset.model.Dataset;
import com.dnastack.ga4gh.search.adapter.model.request.SearchRequest;
import com.dnastack.ga4gh.search.adapter.model.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SearchController {

    @Autowired
    SearchSource dataSource;

    @RequestMapping(value = "/api/search", method = RequestMethod.POST)
    public Dataset search(@RequestBody SearchRequest request, @RequestParam(required = false) Integer pageSize) {
        return dataSource.search(request,pageSize);
    }
}
