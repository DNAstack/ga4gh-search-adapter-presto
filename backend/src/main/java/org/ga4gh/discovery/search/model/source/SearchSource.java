package org.ga4gh.discovery.search.model.source;

import java.util.List;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.ListDatasetsResponse;
import org.ga4gh.dataset.model.ListSchemasResponse;
import org.ga4gh.dataset.model.Schema;
import org.ga4gh.discovery.search.model.Field;
import org.ga4gh.discovery.search.model.Table;
import org.ga4gh.discovery.search.model.request.SearchRequest;

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
