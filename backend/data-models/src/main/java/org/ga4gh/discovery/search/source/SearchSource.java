package org.ga4gh.discovery.search.source;

import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.discovery.search.request.SearchRequest;
import org.ga4gh.discovery.search.result.SearchResult;

import java.util.List;
import java.util.Map;

/** @author mfiume */
public interface SearchSource {

    List<Table> getTables();

    Map<String, List<Table>> getDatasets();

    Dataset getDataset(String id);

    List<Field> getFields(String table);

    SearchResult search(SearchRequest query);


}
