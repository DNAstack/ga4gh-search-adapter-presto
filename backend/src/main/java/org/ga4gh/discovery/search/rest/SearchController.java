package org.ga4gh.discovery.search.rest;

import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.discovery.search.model.request.SearchRequest;
import org.ga4gh.discovery.search.model.source.SearchSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SearchController {

    @Autowired
    SearchSource dataSource;

    @RequestMapping(value = "/api/search", method = RequestMethod.POST)
    public Dataset search(@RequestBody SearchRequest request) {
        return dataSource.search(request);
    }
}
