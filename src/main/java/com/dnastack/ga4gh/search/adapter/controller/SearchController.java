package com.dnastack.ga4gh.search.adapter.controller;

import com.dnastack.ga4gh.search.adapter.api.SearchSource;
import com.dnastack.ga4gh.search.adapter.model.SearchRequest;
import com.dnastack.ga4gh.search.adapter.model.TableData;
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
    public TableData search(@RequestBody SearchRequest request, @RequestParam(required = false) Integer pageSize) {
        return dataSource.search(request, pageSize);
    }
}
