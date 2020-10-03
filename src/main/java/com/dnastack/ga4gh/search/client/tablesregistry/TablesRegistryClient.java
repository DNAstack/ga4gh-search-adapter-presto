package com.dnastack.ga4gh.search.client.tablesregistry;

import com.dnastack.ga4gh.search.client.tablesregistry.model.ListTableRegistryEntry;
import feign.Param;
import feign.RequestLine;

public interface TablesRegistryClient {
    @RequestLine("GET /api/registry/{userId}/collections?table_identifier={tableIdentifier}")
    ListTableRegistryEntry getTableRegistryEntry(@Param("userId") String userId,
                                                 @Param("tableIdentifier") String tableIdentifier);
}
