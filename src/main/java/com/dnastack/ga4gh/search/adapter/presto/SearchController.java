package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.shared.DeferredResultUtils;
import com.dnastack.ga4gh.search.tables.TableData;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

@RestController
public class SearchController {

    @Autowired
    PrestoClient prestoClient;

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public DeferredResult<TableData> search(@RequestBody SearchRequest request,
        @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        return DeferredResultUtils
            .ofSingle(() -> new SearchAdapter(prestoClient, parseCredentialsHeader(clientSuppliedCredentials))
                .search(request.getSqlQuery()));
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search/**", method = RequestMethod.GET)
    public DeferredResult<TableData> getNextPaginatedResponse(HttpServletRequest request,
        @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {

        return DeferredResultUtils
            .ofSingle(() -> {

                String page = request.getRequestURI()
                    .split(request.getContextPath() + "/search/")[1];
                return new SearchAdapter(prestoClient, parseCredentialsHeader(clientSuppliedCredentials))
                    .getNextPage(page);
            });
    }

    // TODO make this method into a Spring MVC parameter provider
    public static Map<String, String> parseCredentialsHeader(List<String> clientSuppliedCredentials) {
        return clientSuppliedCredentials.stream()
            .map(val -> val.split("=", 2))
            .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

}
