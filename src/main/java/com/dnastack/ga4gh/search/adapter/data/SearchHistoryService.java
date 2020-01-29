package com.dnastack.ga4gh.search.adapter.data;

public interface SearchHistoryService {

    static final Integer DEFAULT_PAGE_SIZE = 10;

    void addSearchHistory(String sql, Boolean succeeded);

    ListSearchHistory getSearchHistory(Integer page, Integer pageSize);

}
