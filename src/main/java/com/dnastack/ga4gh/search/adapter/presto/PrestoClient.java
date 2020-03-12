package com.dnastack.ga4gh.search.adapter.presto;

import com.fasterxml.jackson.databind.JsonNode;

//TODO: get rid of?
public interface PrestoClient {
    JsonNode query(String statement);
    JsonNode next(String page);
}
