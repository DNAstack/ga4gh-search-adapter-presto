package org.ga4gh.discovery.search.presto;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Consumer;

public interface PrestoAdapter {

    PrestoTableMetadata getMetadata(PrestoTable table);

    void query(String prestoSQL, Consumer<ResultSet> resultProcessor);
    void query(String prestoSQL,List<Object> params, Consumer<ResultSet> resultProcessor);
}
