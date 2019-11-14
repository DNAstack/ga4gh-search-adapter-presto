package com.dnastack.ga4gh.search.adapter.controller;

import com.dnastack.ga4gh.search.adapter.model.TableData;
import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class DsResultController {

    @Autowired
    PrestoSearchSource dataSource;

    @RequestMapping(value = "/api/dsresults/{nextPageToken}", method = RequestMethod.GET)
    public TableData getNextPaginatedResposne(@PathVariable("nextPageToken") String nextPageToken) {
        return dataSource.getPaginatedResponse(nextPageToken);

    }

}
