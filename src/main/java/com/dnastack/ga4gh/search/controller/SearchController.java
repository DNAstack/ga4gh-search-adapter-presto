package com.dnastack.ga4gh.search.controller;

import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchAdapter;
import com.dnastack.ga4gh.search.adapter.presto.SearchRequest;
import com.dnastack.ga4gh.search.model.TableData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class SearchController {

    @Autowired
    private PrestoSearchAdapter prestoSearchAdapter;

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public TableData search(@RequestBody SearchRequest searchRequest,
                            HttpServletRequest request,
                            @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        return prestoSearchAdapter
                .search(searchRequest.getSqlQuery(), request, parseCredentialsHeader(clientSuppliedCredentials));
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search/**", method = RequestMethod.GET)
    public TableData getNextPaginatedResponse(@RequestParam("queryJobId") String queryJobId,
                                              HttpServletRequest request,
                                              @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        String page = request.getRequestURI()
                             .split(request.getContextPath() + "/search/")[1];
        TableData tableData = prestoSearchAdapter
                .getNextSearchPage(page, queryJobId, request, parseCredentialsHeader(clientSuppliedCredentials));
        return tableData;
    }

    // TODO make this method into a Spring MVC parameter provider
    public static Map<String, String> parseCredentialsHeader(List<String> clientSuppliedCredentials) {
        return clientSuppliedCredentials.stream()
            .map(val -> val.split("=", 2))
            .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

}
