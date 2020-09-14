package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.tables.ColumnSchema;

public interface Ga4ghTypeTransformer {
    String transform(ColumnSchema columnSchema);
}
