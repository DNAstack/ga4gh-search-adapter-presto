package com.dnastack.ga4gh.search.client.indexingservice;

import com.dnastack.ga4gh.search.client.indexingservice.model.LibraryItem;
import feign.Param;
import feign.RequestLine;

public interface IndexingServiceClient {
    @RequestLine("GET /library/{objectId}")
    LibraryItem getBlob(@Param("objectId") String objectId);
}
