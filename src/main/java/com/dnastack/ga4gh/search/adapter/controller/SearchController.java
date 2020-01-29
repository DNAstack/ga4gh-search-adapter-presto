package com.dnastack.ga4gh.search.adapter.controller;

import com.dnastack.ga4gh.search.adapter.data.SearchHistoryService;
import com.dnastack.ga4gh.search.adapter.data.ListSearchHistory;
import com.dnastack.ga4gh.search.adapter.model.SearchRequest;
import com.dnastack.ga4gh.search.adapter.model.TableData;
import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SearchController {

    @Autowired
    PrestoSearchSource dataSource;

    @Autowired
    SearchHistoryService searchHistoryService;

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public TableData search(@RequestBody SearchRequest request, @RequestParam(required = false) Integer pageSize) {
        return dataSource.search(request, pageSize);
    }

    @RequestMapping(value = "/search/history", method = RequestMethod.GET)
    public ListSearchHistory search(@RequestParam(value = "page_size", required = false) Integer pageSize, @RequestParam(required = false,value = "page") Integer page) {
        return searchHistoryService.getSearchHistory(page, pageSize);
    }
}
