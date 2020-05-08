package com.dnastack.ga4gh.search.adapter.telemetry;

import com.dnastack.ga4gh.search.adapter.presto.PrestoClient;
import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.reactivex.rxjava3.core.Single;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

    public Single<JsonNode> query(String statement, Map<String, String> extraCredentials) {
        return Single.defer(() -> {
            queryCount.increment();
            long start = System.currentTimeMillis();
            return client.query(statement, extraCredentials)
                .doOnSuccess((node) -> {
                    queryLatency.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                });
        });
    }

    public Single<JsonNode> next(String page, Map<String, String> extraCredentials) {
        return Single.defer(() -> {
            queryCount.increment();
            long start = System.currentTimeMillis();
            return client.next(page, extraCredentials)
                .doOnSuccess((node) -> {
                    queryLatency.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                });
        });
    }
}
