package com.dnastack.ga4gh.search.adapter.presto;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.Map;

//TODO: get rid of?
public interface PrestoClient {

    /**
     * Runs the given SQL statement, and polls Presto until it either bears a page of results or errors out.
     *
     * @param statement        the SQL statement to execute.
     * @param extraCredentials The extra X-Presto-Extra-Credentials to include in the request.
     * @return The first JSON response from Presto that's either a partial result (even with 0 rows), or a final result.
     * Never null.
     * @throws IOException if HTTP communication with Presto fails or there is a parse error with the JSON response.
     */
    JsonNode query(String statement, Map<String, String> extraCredentials) throws IOException;

    /**
     * Fetches the given page of a running query from Presto, polling until it either bears some results or errors out.
     *
     * @param page             the next page token returned by Presto in a previous call to {@link #query(String, Map)} or to this method.
     * @param extraCredentials The extra X-Presto-Extra-Credentials to include in the request.
     * @return The first JSON response from Presto that's either a partial result (even with 0 rows), or a final result.
     * Never null.
     * @throws IOException if HTTP communication with Presto fails or there is a parse error with the JSON response.
     */
    JsonNode next(String page, Map<String, String> extraCredentials) throws IOException;
}
