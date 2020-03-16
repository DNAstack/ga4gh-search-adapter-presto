package com.dnastack.ga4gh.search.adapter.telemetry;

import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

import java.io.IOException;
import java.util.Map;

/***
 * Wraps a PrestoClient to add telemetry.
 */
public class PrestoTelemetryClient implements PrestoClient {
    protected final Counter queryCount;
    protected final Counter pageCount;
    protected final Timer queryLatency;
    private final PrestoClient client;

    public PrestoTelemetryClient(PrestoClient client) {
        this.client = client;
        this.queryCount = Monitor.registerCounter("search.queries.queries_performed",
                "The raw number of queries performed over a given step of time.");
        this.queryLatency = Monitor.registerRequestTimer("search.queries.query_latency",
                "The average latency of queries performed over a given step of time.");
        this.pageCount = Monitor.registerCounter("search.queries.additional_pages_retrieved",
                "The number of additional pages retrieved after an initial query.");
    }

    public JsonNode query(String statement, Map<String, String> extraCredentials) throws IOException {
        queryCount.increment();
        try {
            return queryLatency.recordCallable(() -> client.query(statement, extraCredentials));
        } catch (final IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Unexpected checked exception", e);
        }
    }

    public JsonNode next(String page, Map<String, String> extraCredentials) throws IOException {
        pageCount.increment();
        return client.next(page, extraCredentials);
    }
}
