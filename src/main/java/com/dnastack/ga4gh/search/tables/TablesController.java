package com.dnastack.ga4gh.search.tables;

import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.dnastack.ga4gh.search.adapter.presto.SearchAdapter;
import com.dnastack.ga4gh.search.adapter.presto.SearchController;
import com.dnastack.ga4gh.search.tables.ListTables;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.io.IOException;
import java.util.List;

@Slf4j
@RestController
public class TablesController {

    @Autowired
    PrestoClient client;

    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public ListTables getTables(@RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) throws IOException {
        return new SearchAdapter(client, SearchController.parseCredentialsHeader(clientSuppliedCredentials))
                .getTables(ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());
    }


    @PreAuthorize("hasAnyAuthority('SCOPE_read:data', 'SCOPE_read:data_model')")
    @RequestMapping(value = "/table/{table_name}/info", method = RequestMethod.GET)
    public TableInfo getTableInfo(@PathVariable("table_name") String tableName,
                                  @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) throws IOException {
        return new SearchAdapter(client, SearchController.parseCredentialsHeader(clientSuppliedCredentials))
                .getTableInfo(tableName, ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());
    }

    @PreAuthorize("hasAuthority('SCOPE_read:data')")
    @RequestMapping(value = "/table/{table_name}/data", method = RequestMethod.GET)
    public TableData getTableData(@PathVariable("table_name") String tableName,
                                  @RequestHeader(value = "GA4GH-Search-Authorization", defaultValue = "") List<String> clientSuppliedCredentials) throws IOException {
        return new SearchAdapter(client, SearchController.parseCredentialsHeader(clientSuppliedCredentials))
                .getTableData(tableName, ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());
    }
}
