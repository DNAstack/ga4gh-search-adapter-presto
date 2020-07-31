package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.tables.TableData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class SearchController {

    @Autowired
    PrestoClient prestoClient;

    private SearchAdapter getSearchAdapter(HttpServletRequest request, List<String> clientSuppliedCredentials){
        return new SearchAdapter(request, prestoClient, parseCredentialsHeader(clientSuppliedCredentials));
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
        @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        String page = request.getRequestURI()
                             .split(request.getContextPath() + "/search/")[1];
        return getSearchAdapter(request, clientSuppliedCredentials)
                .getNextSearchPage(page);
    }

    // TODO make this method into a Spring MVC parameter provider
    public static Map<String, String> parseCredentialsHeader(List<String> clientSuppliedCredentials) {
        return clientSuppliedCredentials.stream()
            .map(val -> val.split("=", 2))
            .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

}
