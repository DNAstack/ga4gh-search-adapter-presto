package com.dnastack.ga4gh.search.adapter.presto;

import java.time.ZonedDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConsumerCacheEntry {

    private final ZonedDateTime expires;
    private final PagingResultSetConsumer pagingResultSetConsumer;

    public ConsumerCacheEntry(PagingResultSetConsumer pagingResultSetConsumer) {
        this(pagingResultSetConsumer, ZonedDateTime.now().plusMinutes(60L));
    }

    public ConsumerCacheEntry(PagingResultSetConsumer pagingResultSetConsumer, ZonedDateTime expires) {
        this.pagingResultSetConsumer = pagingResultSetConsumer;
        this.expires = expires;
    }

    public boolean isExpired() {
        return ZonedDateTime.now().isAfter(expires);
    }
}
