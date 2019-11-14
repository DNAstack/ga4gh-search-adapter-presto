package com.dnastack.ga4gh.search.adapter.presto;

import java.util.List;

public interface PrestoAdapter {

    PagingResultSetConsumer query(String prestoSQL);

    PagingResultSetConsumer query(String prestoSQL, Integer pageSize);

    PagingResultSetConsumer query(String prestoSQL, List<Object> params);

    PagingResultSetConsumer query(String prestoSQL, List<Object> params, Integer pageSize);
}
