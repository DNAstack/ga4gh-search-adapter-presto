package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.shared.DeferredResultUtils;
import com.dnastack.ga4gh.search.adapter.shared.Ga4ghCredentials;
import com.dnastack.ga4gh.search.tables.TableData;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

@RestController
public class SearchController {

    @Autowired
    SearchAdapter searchAdapter;

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public DeferredResult<TableData> search(HttpServletRequest request, @RequestBody SearchRequest searchRequest, @Ga4ghCredentials Map<String, String> clientSuppliedCredentials) {
        return DeferredResultUtils
            .ofSingle(() -> searchAdapter
                .search(request, searchRequest.getSqlQuery(), clientSuppliedCredentials));
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/search/**", method = RequestMethod.GET)
    public DeferredResult<TableData> getNextPaginatedResponse(HttpServletRequest request, @Ga4ghCredentials Map<String, String> clientSuppliedCredentials) {

        return DeferredResultUtils
            .ofSingle(() -> {
                String page = request.getRequestURI()
                    .split(request.getContextPath() + "/search/")[1];
                return searchAdapter.getNextPage(request, page, clientSuppliedCredentials);
            });
    }

}
