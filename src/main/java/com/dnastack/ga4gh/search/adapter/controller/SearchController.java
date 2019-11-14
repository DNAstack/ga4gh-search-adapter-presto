package com.dnastack.ga4gh.search.adapter.controller;

import com.dnastack.ga4gh.search.adapter.model.SearchRequest;
import com.dnastack.ga4gh.search.adapter.model.TableData;
import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class SearchController {

    @Autowired
    PrestoSearchSource dataSource;

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public TableData search(@RequestBody SearchRequest request, @RequestParam(required = false) Integer pageSize) {
        return dataSource.search(request, pageSize);
    }
}
