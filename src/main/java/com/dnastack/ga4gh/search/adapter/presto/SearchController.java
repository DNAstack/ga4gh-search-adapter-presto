package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.tables.TableData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
public class SearchController {

    @Autowired
    SearchAdapter searchAdapter;

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public TableData search(@RequestBody SearchRequest request) {
        return searchAdapter.search(request.getSqlQuery());
    }

    @RequestMapping(value = "/search/**", method = RequestMethod.GET)
    public TableData getNextPaginatedResponse(HttpServletRequest request) {
        //todo: better way?
        String page = request.getRequestURI()
                .split(request.getContextPath() + "/search/")[1];
        return searchAdapter.getNextPage(page);

    }
}
