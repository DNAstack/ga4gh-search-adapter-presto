package com.dnastack.ga4gh.search.adapter.controller;

import com.dnastack.ga4gh.search.adapter.api.SearchSource;
import com.dnastack.ga4gh.search.adapter.model.ListTableResponse;
import com.dnastack.ga4gh.search.adapter.model.Table;
import com.dnastack.ga4gh.search.adapter.model.TableData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class TablesController {

    @Autowired
    SearchSource dataSource;

    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public ListTableResponse getTables() {
        return dataSource.getTables();
    }


    @RequestMapping(value = "/table/{table_name}/info", method = RequestMethod.GET)
    public Table getTableInfo(@PathVariable("table_name") String tableName) {
        return dataSource.getTable(tableName);
    }


    @RequestMapping(value = "/table/{table_name}/data", method = RequestMethod.GET)
    public TableData getTableData(@PathVariable("table_name") String tableName, @RequestParam(value = "pageSize", required = false) Integer pageSize) {
        return dataSource.getTableData(tableName, pageSize);
    }
}
