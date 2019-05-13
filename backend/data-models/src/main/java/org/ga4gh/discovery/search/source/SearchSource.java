package org.ga4gh.discovery.search.source;

import java.util.List;

import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.request.SearchRequest;
import org.ga4gh.discovery.search.result.SearchResult;

/** @author mfiume */
public interface SearchSource {

    List<Table> getTables();

    List<Field> getFields(String table);

    SearchResult search(SearchRequest query);
}
