package com.dnastack.ga4gh.search.adapter.data;

import com.dnastack.ga4gh.search.adapter.data.SearchHistory;
import com.dnastack.ga4gh.search.adapter.model.Pagination;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ListSearchHistory {

    @JsonProperty("search_history")
    private List<SearchHistory> searchHistory;

    @JsonProperty("pagination")
    private Pagination pagination;


}
