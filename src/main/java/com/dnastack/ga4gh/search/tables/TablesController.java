package com.dnastack.ga4gh.search.tables;

import com.dnastack.ga4gh.search.adapter.presto.SearchAdapter;
import com.dnastack.ga4gh.search.tables.ListTables;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Slf4j
@RestController
public class TablesController {

    @Autowired
    SearchAdapter dataSource; // TODO: this is weirdly circular.

    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public ListTables getTables() {
        return dataSource.getTables(ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());
    }


    @RequestMapping(value = "/table/{table_name}/info", method = RequestMethod.GET)
    public TableInfo getTableInfo(@PathVariable("table_name") String tableName) {
        return dataSource.getTableInfo(tableName, ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());
    }

    @RequestMapping(value = "/table/{table_name}/data", method = RequestMethod.GET)
    public TableData getTableData(@PathVariable("table_name") String tableName) {
        return dataSource.getTableData(tableName, ServletUriComponentsBuilder.fromCurrentContextPath().toUriString());
    }
}
