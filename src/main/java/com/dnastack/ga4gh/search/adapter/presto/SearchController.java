package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.tables.TableData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class SearchController {

    @Autowired
    PrestoClient prestoClient;

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public TableData search(@RequestBody SearchRequest request,
                            @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) throws IOException {
        return new SearchAdapter(prestoClient, parseCredentialsHeader(clientSuppliedCredentials))
                .search(request.getSqlQuery());
    }

    @RequestMapping(value = "/search/**", method = RequestMethod.GET)
    public TableData getNextPaginatedResponse(HttpServletRequest request,
                                              @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) throws IOException {

        String page = request.getRequestURI()
                .split(request.getContextPath() + "/search/")[1];
        return new SearchAdapter(prestoClient, parseCredentialsHeader(clientSuppliedCredentials))
                .getNextPage(page);
    }

    // TODO make this method into a Spring MVC parameter provider
    public static Map<String, String> parseCredentialsHeader(List<String> clientSuppliedCredentials) {
        return clientSuppliedCredentials.stream()
                .map(val -> val.split("=", 2))
                .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
    }

}
