package com.dnastack.ga4gh.search.tables;

import com.dnastack.ga4gh.search.adapter.presto.SearchAdapter;
import com.dnastack.ga4gh.search.adapter.shared.DeferredResultUtils;
import com.dnastack.ga4gh.search.adapter.shared.Ga4ghCredentials;
import com.dnastack.ga4gh.search.tables.TableError.ErrorCode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Slf4j
@RestController
public class TablesController {


    @Autowired
    SearchAdapter searchAdapter;

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public DeferredResult<ResponseEntity<ListTables>> getTables(HttpServletRequest request, @Ga4ghCredentials Map<String, String> clientSuppliedCredentials) throws IOException {
        return DeferredResultUtils.ofSingle(() -> searchAdapter
            .getTables(request, ServletUriComponentsBuilder.fromCurrentContextPath()
                .toUriString(), clientSuppliedCredentials)
            .map(listTables -> ResponseEntity.ok().headers(getExtraAuthHeaders(listTables)).body(listTables)));
    }

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/table/{table_name}/info", method = RequestMethod.GET)
    public DeferredResult<TableInfo> getTableInfo(HttpServletRequest request, @PathVariable("table_name") String tableName,
        @Ga4ghCredentials Map<String, String> clientSuppliedCredentials) {
        return DeferredResultUtils.ofSingle(() -> searchAdapter
            .getTableInfo(request, tableName, ServletUriComponentsBuilder.fromCurrentContextPath()
                .toUriString(), clientSuppliedCredentials));
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/table/{table_name}/data", method = RequestMethod.GET)
    public DeferredResult<TableData> getTableData(HttpServletRequest request, @PathVariable("table_name") String tableName,
        @Ga4ghCredentials Map<String, String> clientSuppliedCredentials) {
        return DeferredResultUtils.ofSingle(() -> searchAdapter
            .getTableData(request, tableName, ServletUriComponentsBuilder.fromCurrentContextPath()
                .toUriString(), clientSuppliedCredentials)
        );
    }

    private static String escapeQuotes(String s) {
        return s.replace("\"", "\\\"");
    }

    private HttpHeaders getExtraAuthHeaders(ListTables listTables) {
        HttpHeaders headers = new HttpHeaders();
        List<TableError> errors = listTables.getErrors();
        if (errors != null) {
            errors.stream().filter(error -> error.getCode().equals(ErrorCode.AUTH_CHALLENGE))
                .forEach(authError -> {
                    headers.add("WWW-Authenticate",
                        "GA4GH-Search realm:\"" + escapeQuotes(authError.getSource()) + "\"");
                });
        }
        return headers;
    }
}
