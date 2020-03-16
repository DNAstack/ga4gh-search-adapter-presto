package com.dnastack.ga4gh.search.adapter.presto;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Map;

//TODO: get rid of?
public interface PrestoClient {
    JsonNode query(String statement, Map<String, String> extraCredentials) throws IOException;
    JsonNode next(String page, Map<String, String> extraCredentials) throws IOException;
}
