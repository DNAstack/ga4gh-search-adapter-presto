package com.dnastack.ga4gh.search.adapter.model.source;

import com.dnastack.ga4gh.search.adapter.model.Field;
import com.dnastack.ga4gh.search.adapter.model.Table;
import com.dnastack.ga4gh.search.adapter.model.request.SearchRequest;
import java.util.List;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Schema;

/**
 * @author mfiume
 */
public interface SearchSource {

    List<Table> getTables();

    ListSchemasResponse getSchemas();

    Schema getSchema(String id);

    ListDatasetsResponse getDatasets();

    Dataset getDataset(String id, Integer pageSize);

    List<Field> getFields(String table);

    Dataset search(SearchRequest query, Integer pageSize);

    Dataset getPaginatedResponse(String token);

}
