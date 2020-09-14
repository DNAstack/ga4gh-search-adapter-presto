package com.dnastack.ga4gh.search;

import com.dnastack.ga4gh.search.ApplicationConfig;
import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchAdapter;
import com.dnastack.ga4gh.search.adapter.presto.SearchRequest;
import com.dnastack.ga4gh.search.repository.QueryJobRepository;
import com.dnastack.ga4gh.search.tables.TableData;
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
    private PrestoClient prestoClient;

    @Autowired
    private ApplicationConfig applicationConfig;

    @Autowired
    private QueryJobRepository queryJobRepository;

    private PrestoSearchAdapter getSearchAdapter(HttpServletRequest request, List<String> clientSuppliedCredentials){
        return new PrestoSearchAdapter(request, prestoClient, parseCredentialsHeader(clientSuppliedCredentials), applicationConfig.getHiddenCatalogs(), queryJobRepository);
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public TableData search(HttpServletRequest request, @RequestBody SearchRequest searchRequest,
        @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        return getSearchAdapter(request, clientSuppliedCredentials).search(searchRequest.getSqlQuery());
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search/**", method = RequestMethod.GET)
    public TableData getNextPaginatedResponse(HttpServletRequest request,
        @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials,
                                              @RequestParam("queryJobId") String queryJobId) {
        String page = request.getRequestURI()
                             .split(request.getContextPath() + "/search/")[1];
        TableData tableData = getSearchAdapter(request, clientSuppliedCredentials)
                .getNextSearchPage(page, queryJobId);
        return tableData;
    }

    // TODO make this method into a Spring MVC parameter provider
    public static Map<String, String> parseCredentialsHeader(List<String> clientSuppliedCredentials) {
        return clientSuppliedCredentials.stream()
            .map(val -> val.split("=", 2))
            .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

}
