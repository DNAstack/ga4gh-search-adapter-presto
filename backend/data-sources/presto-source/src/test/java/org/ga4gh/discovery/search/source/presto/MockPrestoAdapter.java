package org.ga4gh.discovery.search.source.presto;

import static com.google.common.base.Preconditions.checkArgument;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/** Simulates presto responses */
public class MockPrestoAdapter implements PrestoAdapter {

    private final Map<String, PrestoTableMetadata> metadata;
    private Map<String, ResultSet> mockResultSests = new HashMap<String, ResultSet>();

    public MockPrestoAdapter(Map<String, PrestoTableMetadata> metadata) {
        this.metadata = metadata;
    }

    @Override
    public PrestoTableMetadata getMetadata(PrestoTable table) {
        PrestoTableMetadata metadata = this.metadata.get(table.getName());
        checkArgument(metadata != null, "table " + table.getName() + " doesn't exist");
        return metadata;
    }

    @Override
    public void query(String prestoSQL, Consumer<ResultSet> resultProcessor) {
        ResultSet resultSet = mockResultSests.get(prestoSQL);
        checkArgument(resultSet != null, "No mock result set for query '" + prestoSQL + "'");
        resultProcessor.accept(resultSet);
    }

    public void addMockResults(String sql, ResultSet resultSet) {
        mockResultSests.put(sql, resultSet);
    }
}
