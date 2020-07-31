package com.dnastack.ga4gh.search.adapter.presto;

import lombok.Value;

@Value
public class PrestoNoSuchCatalogException extends RuntimeException {
    private final String catalog;
}
