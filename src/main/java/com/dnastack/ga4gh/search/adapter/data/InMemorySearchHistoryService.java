package com.dnastack.ga4gh.search.adapter.data;

import com.dnastack.ga4gh.search.adapter.model.Pagination;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;

public class InMemorySearchHistoryService implements SearchHistoryService {


    private int maxRetainedSearches;
    private LinkedList<SearchHistory> history;

    public InMemorySearchHistoryService(int maxRetainedSearches) {
        this.maxRetainedSearches = maxRetainedSearches;
        history = new LinkedList<>();
    }

    @Override
    public void addSearchHistory(String sql, Boolean succeeded) {
        SearchHistory searchHistory = new SearchHistory();
        searchHistory.setSucceeded(Optional.ofNullable(succeeded).orElse(false));
        searchHistory.setSubmissionDate(ZonedDateTime.now());
        searchHistory.setSqlQuery(sql);

        history.push(searchHistory);
        if (history.size() > maxRetainedSearches) {
            history.pop();
        }
    }

    @Override
    public ListSearchHistory getSearchHistory(Integer page, Integer pageSize) {
        if (page == null) {
            page = 0;
        }
        if (pageSize == null) {
            pageSize = SearchHistoryService.DEFAULT_PAGE_SIZE;
        }

        ListSearchHistory searchHistory = new ListSearchHistory();
        int totalResults = history.size();
        if (page * pageSize > totalResults) {
            searchHistory.setSearchHistory(Collections.emptyList());
        } else {
            int offset = page * pageSize;
            int maxIndex = Math.min(offset + pageSize, totalResults);
            searchHistory.setSearchHistory(history.subList(offset, maxIndex));

            Pagination pagination = new Pagination();

            if (offset > 0) {
                URI previous = URI
                    .create(String.format("history?" + String.format("page=%d&page_size=%d", page - 1, pageSize)));
                pagination.setPreviousPageUrl(previous);
            }

            if (totalResults > offset + pageSize) {
                URI next = URI.create("history?" + String.format("page=%d&page_size=%d", page + 1, pageSize));
                pagination.setNextPageUrl(next);
            }
            searchHistory.setPagination(pagination);

        }
        return searchHistory;
    }

}
