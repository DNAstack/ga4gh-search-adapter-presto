package org.ga4gh.discovery.search.rest;

import org.ga4gh.discovery.search.result.SearchResult;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMethod;

@RestController
public class SearchController {

    @Autowired
    SearchSource dataSource;
    
    @CrossOrigin(origins = "${cors.url}")
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public SearchResult search(@RequestBody SearchQuery query) {
        return dataSource.search(query);
    }
}
