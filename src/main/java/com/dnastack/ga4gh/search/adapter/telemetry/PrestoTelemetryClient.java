package com.dnastack.ga4gh.search.adapter.telemetry;

import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

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

    public JsonNode query(String statement) {
        queryCount.increment();
        return queryLatency.record(() -> client.query(statement));
    }

    public JsonNode next(String page) {
        pageCount.increment();
        return client.next(page);
    }
}
