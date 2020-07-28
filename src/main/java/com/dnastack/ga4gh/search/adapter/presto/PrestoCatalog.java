package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.shared.AuthRequiredException;
import com.dnastack.ga4gh.search.adapter.shared.SearchAuthRequest;
import com.dnastack.ga4gh.search.tables.TableData;
import com.dnastack.ga4gh.search.tables.TableError;
import com.dnastack.ga4gh.search.tables.TableInfo;
import com.dnastack.ga4gh.search.tables.TablesList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class PrestoCatalog {
    private final SearchAdapter searchAdapter;
    private final String refHost;
    private final String catalogName;

    private static final String QUERY_TABLE_TEMPLATE =
            "SELECT table_catalog, table_schema, table_name" +
            " FROM %s.information_schema.tables" +
            " WHERE table_schema != 'information_schema'" +
            " ORDER BY 1, 2, 3";

    private TableInfo getTableInfo(Map<String, Object> row){
        String schema = (String) row.get("table_schema");
        String table = (String) row.get("table_name");
        String qualifiedTableName = catalogName + "." + schema + "." + table;
        String ref = String.format("%s/table/%s/info", refHost, qualifiedTableName);
        Map<String, Object> dataModel = new HashMap<>();
        dataModel.put("$ref", ref);
        log.trace("Got table "+qualifiedTableName);
        return new TableInfo(qualifiedTableName, null, dataModel);
    }

    private List<TableInfo> getTableInfoList(TableData tableData){
        return tableData.getData().stream()
                        .map(this::getTableInfo)
                        .collect(Collectors.toList());
    }

    private List<TableInfo> combineTableInfo(List<List<TableInfo>> tableInfoLists){
        return tableInfoLists.stream()
                             .flatMap(innerList->innerList.stream())
                             .collect(Collectors.toList());
    }

    private static String quote(String sqlIdentifier) {
        return "\"" + sqlIdentifier.replace("\"", "\"\"") + "\"";
    }


    public TablesList getTablesList(){
        try {
            TableData tables = searchAdapter.searchAll(String.format(QUERY_TABLE_TEMPLATE, quote(catalogName)));
            List<TableInfo> tableInfoList = getTableInfoList(tables);
            return new TablesList(tableInfoList, null, null);
        }catch(AuthRequiredException ex){
            SearchAuthRequest searchAuthRequest = ex.getAuthorizationRequest();
            TableError error = new TableError();
            error.setMessage("User is not authorized to access catalog: " + searchAuthRequest.getKey()
                             + ", request requires additional authorization information");
            error.setSource(searchAuthRequest.getKey());
            error.setCode(TableError.ErrorCode.AUTH_CHALLENGE);
            error.setAttributes(searchAuthRequest.getResourceDescription());
            if(log.isTraceEnabled()){
                log.error("Error when fetching tables for {}", catalogName, ex);
            }
            return new TablesList(null, List.of(error), null);
        }catch(PrestoQueryFailedException | PrestoIOException ex){
            TableError error = new TableError();
            error.setMessage("Couldn't complete query to list tables");
            error.setSource(catalogName);
            error.setCode(TableError.ErrorCode.PRESTO_QUERY);
            if(log.isTraceEnabled()){
                log.error("Error when fetching tables for {}", catalogName, ex);
            }
            return new TablesList(null, List.of(error), null);
        }
    }


}
