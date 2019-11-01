package com.dnastack.ga4gh.search.adapter.presto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PagingResultSetConsumerCache {

    private final Map<String, ConsumerCacheEntry> consumerCache;
    private final TaskScheduler scheduler;


    @Autowired
    public PagingResultSetConsumerCache(TaskScheduler scheduler) {
        this.consumerCache = new ConcurrentHashMap<>();
        this.scheduler = scheduler;
        scheduler.scheduleWithFixedDelay(this::cleanCache, 10000L);
    }


    private void cleanCache() {
        log.trace("Cleaning Consumer cache");
        List<String> clearIds = new ArrayList<>();
        for (Entry<String, ConsumerCacheEntry> cacheEntry : consumerCache.entrySet()) {
            if (cacheEntry.getValue().isExpired()) {
                clearIds.add(cacheEntry.getKey());
            }
        }

        if (!clearIds.isEmpty()) {
            log.trace("Removing " + clearIds.size() + " cache entries");
        }
        clearIds.forEach((id) -> {
            ConsumerCacheEntry cacheEntry = consumerCache.get(id);
            cacheEntry.getPagingResultSetConsumer().close();
            consumerCache.remove(id);
        });

    }

    public PagingResultSetConsumer get(String consumerId) {
        ConsumerCacheEntry cacheEntry = consumerCache.get(consumerId);
        if (cacheEntry.isExpired()) {
            throw new InvalidCacheEntry("Cache entry is expired. Please resubmit search");
        }
        if (cacheEntry == null) {
            throw new InvalidCacheEntry("Could not retrieve result set for consumer. Search either does not exist or cache entry has epxired");
        }
        return cacheEntry.getPagingResultSetConsumer();
    }

    public void add(PagingResultSetConsumer resultSetConsumer) {
        ConsumerCacheEntry cacheEntry = new ConsumerCacheEntry(resultSetConsumer);
        consumerCache.put(resultSetConsumer.getConsumerId(), cacheEntry);
    }

}
