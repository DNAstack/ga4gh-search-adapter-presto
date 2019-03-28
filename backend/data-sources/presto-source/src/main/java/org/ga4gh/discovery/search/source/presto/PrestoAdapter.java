package org.ga4gh.discovery.search.source.presto;

import java.sql.ResultSet;
import java.util.function.Consumer;

public interface PrestoAdapter {

    PrestoTableMetadata getMetadata(PrestoTable table);

    void query(String prestoSQL, Consumer<ResultSet> resultProcessor);
}
