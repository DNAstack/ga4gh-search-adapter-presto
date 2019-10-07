package com.dnastack.ga4gh.search.adapter.presto;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.NOT_FOUND)
public class InvalidCacheEntry extends RuntimeException {


    public InvalidCacheEntry(String message) {
        super(message);
    }
}
