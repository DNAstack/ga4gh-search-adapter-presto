package com.dnastack.ga4gh.search.adapter.api;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.SearchRequest;
import com.dnastack.ga4gh.search.adapter.model.ListTableResponse;
import com.dnastack.ga4gh.search.adapter.model.Table;
import com.dnastack.ga4gh.search.adapter.model.TableData;
import java.util.List;

/**
 * @author mfiume
 */
public interface SearchSource {


    ListTableResponse getTables();

    Table getTable(String tableName);

    TableData getTableData(String tableName, Integer pageSize);

    TableData getPaginatedResponse(String token);

    TableData search(SearchRequest query, Integer pageSize);

    List<Field> getFields(String table);


}
