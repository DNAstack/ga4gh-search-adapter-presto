package org.ga4gh.discovery.search.source;

import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Schema;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.request.SearchRequest;

import java.util.List;

/** @author mfiume */
public interface SearchSource {

    List<Table> getTables();

    ListSchemasResponse getSchemas();

    Schema getSchema(String id);

    ListDatasetsResponse getDatasets();

    Dataset getDataset(String id);

    List<Field> getFields(String table);

    Dataset search(SearchRequest query);


}
