package com.dnastack.ga4gh.search.controller;

import com.dnastack.ga4gh.search.adapter.presto.PrestoSearchAdapter;
import com.dnastack.ga4gh.search.adapter.presto.exception.TableApiErrorException;
import com.dnastack.ga4gh.search.model.TableData;
import com.dnastack.ga4gh.search.model.TableError;
import com.dnastack.ga4gh.search.model.TableError.ErrorCode;

import java.util.List;
import javax.servlet.http.HttpServletRequest;

import com.dnastack.ga4gh.search.model.TableInfo;
import com.dnastack.ga4gh.search.model.TablesList;
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

@Slf4j
@RestController
public class TablesController {

    @Autowired
    private PrestoSearchAdapter prestoSearchAdapter;

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public ResponseEntity<TablesList> getTables(HttpServletRequest request,
                                                @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        TablesList tablesList = null;

        try {
            tablesList = prestoSearchAdapter
                .getTables(request, SearchController.parseCredentialsHeader(clientSuppliedCredentials));
        } catch (Exception ex) {
            throw new TableApiErrorException(ex, TablesList.class);
        }

        return ResponseEntity.ok().headers(getExtraAuthHeaders(tablesList)).body(tablesList);
    }

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/tables/catalog/{catalogName}", method = RequestMethod.GET)
    public ResponseEntity<TablesList> getTablesByCatalog(@PathVariable("catalogName") String catalogName,
                                                         HttpServletRequest request,
                                                         @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {
        TablesList tablesList = null;

        try {
            tablesList = prestoSearchAdapter
                .getTablesInCatalog(catalogName, request, SearchController.parseCredentialsHeader(clientSuppliedCredentials));
        } catch (Exception ex) {
            throw new TableApiErrorException(ex, TablesList.class);
        }

        return ResponseEntity.ok().headers(getExtraAuthHeaders(tablesList)).body(tablesList);

    }

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/table/{table_name}/info", method = RequestMethod.GET)
    public TableInfo getTableInfo(@PathVariable("table_name") String tableName,
                                  HttpServletRequest request,
                                  @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {

        TableInfo tableInfo = null;

        try {
            tableInfo = prestoSearchAdapter
                .getTableInfo(tableName, request, SearchController.parseCredentialsHeader(clientSuppliedCredentials));
        } catch (Exception ex) {
            throw new TableApiErrorException(ex, TableInfo.class);
        }

        return tableInfo;
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/table/{table_name}/data", method = RequestMethod.GET)
    public TableData getTableData(@PathVariable("table_name") String tableName,
                                  HttpServletRequest request,
                                  @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) {

        TableData tableData = null;

        try {
            tableData = prestoSearchAdapter
                .getTableData(tableName, request, SearchController.parseCredentialsHeader(clientSuppliedCredentials));
        } catch (Exception ex) {
            throw new TableApiErrorException(ex, TableData.class);
        }

        return tableData;
    }

    private static String escapeQuotes(String s) {
        return s.replace("\"", "\\\"");
    }

    private HttpHeaders getExtraAuthHeaders(TablesList listTables) {
        HttpHeaders headers = new HttpHeaders();
        TableError error = listTables.getError();
        if (error != null && error.getCode().equals(ErrorCode.AUTH_CHALLENGE)) {
            headers.add("WWW-Authenticate",
                        "GA4GH-Search realm:\"" + escapeQuotes(error.getSource()) + "\"");
        }
        return headers;
    }
}
