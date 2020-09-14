package com.dnastack.ga4gh.search;

import com.dnastack.ga4gh.search.ApplicationConfig;
import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchAdapter;
import com.dnastack.ga4gh.search.SearchController;
import com.dnastack.ga4gh.search.repository.QueryJobRepository;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableError;
import com.dnastack.ga4gh.search.tables.TableError.ErrorCode;

import java.util.List;
import javax.servlet.http.HttpServletRequest;

import com.dnastack.ga4gh.search.tables.TableInfo;
import com.dnastack.ga4gh.search.tables.TablesList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Slf4j
@RestController
public class TablesController {

    @Autowired
    private PrestoClient prestoClient;

    @Autowired
    private ApplicationConfig applicationConfig;

    @Autowired
    private QueryJobRepository queryJobRepository;

    private PrestoSearchAdapter getSearchAdapter(HttpServletRequest request, List<String> clientSuppliedCredentials){
        return new PrestoSearchAdapter(request, prestoClient, SearchController.parseCredentialsHeader(clientSuppliedCredentials), applicationConfig.getHiddenCatalogs(), queryJobRepository);
    }

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public ResponseEntity<TablesList> getTables(HttpServletRequest request, @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        TablesList tablesList = getSearchAdapter(request, clientSuppliedCredentials).getTables();

        return ResponseEntity.ok().headers(getExtraAuthHeaders(tablesList)).body(tablesList);
    }

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/tables/catalog/{catalogName}", method = RequestMethod.GET)
    public ResponseEntity<TablesList> getTablesByCatalog(HttpServletRequest request, @PathVariable("catalogName") String catalogName, @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        TablesList tablesList = getSearchAdapter(request, clientSuppliedCredentials).getTablesInCatalog(catalogName);
        return ResponseEntity.ok().headers(getExtraAuthHeaders(tablesList)).body(tablesList);

    }

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/table/{table_name}/info", method = RequestMethod.GET)
    public TableInfo getTableInfo(HttpServletRequest request, @PathVariable("table_name") String tableName,
                                  @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {

        return getSearchAdapter(request, clientSuppliedCredentials)
                .getTableInfo(tableName, ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());

    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/table/{table_name}/data", method = RequestMethod.GET)
    public TableData getTableData(HttpServletRequest request, @PathVariable("table_name") String tableName,
                                  @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {

        return getSearchAdapter(request, clientSuppliedCredentials)
                .getTableData(tableName, ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());

    }

    private static String escapeQuotes(String s) {
        return s.replace("\"", "\\\"");
    }

    private HttpHeaders getExtraAuthHeaders(TablesList listTables) {
        HttpHeaders headers = new HttpHeaders();
        TableError error = listTables.getError();
        if(error != null && error.getCode().equals(ErrorCode.AUTH_CHALLENGE)){
            headers.add("WWW-Authenticate",
                        "GA4GH-Search realm:\"" + escapeQuotes(error.getSource()) + "\"");
        }
        return headers;
    }
}
