package com.dnastack.ga4gh.search.adapter.source.presto;

import static com.google.common.base.Preconditions.checkArgument;

import com.dnastack.ga4gh.search.adapter.presto.PagingResultSetConsumer;
import com.dnastack.ga4gh.search.adapter.presto.PrestoAdapter;
import com.dnastack.ga4gh.search.adapter.presto.PrestoTable;
import com.dnastack.ga4gh.search.adapter.presto.PrestoTableMetadata;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simulates presto responses
 */
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
    public PagingResultSetConsumer query(String prestoSQL) {
        return query(prestoSQL, 100);
    }


    @Override
    public PagingResultSetConsumer query(String prestoSQL, Integer pageSize) {
        try {
            ResultSet resultSet = mockResultSests.get(prestoSQL);
            checkArgument(resultSet != null, "No mock result set for query '" + prestoSQL + "'");
            return new PagingResultSetConsumer(resultSet, 100);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PagingResultSetConsumer query(String prestoSQL, List<Object> params) {
        return query(prestoSQL, params, 100);
    }


    @Override
    public PagingResultSetConsumer query(String prestoSQL, List<Object> params, Integer pageSize) {
        try {
            ResultSet resultSet = mockResultSests.get(prestoSQL);
            checkArgument(resultSet != null, "No mock result set for query '" + prestoSQL + "'");
            return new PagingResultSetConsumer(resultSet, 100);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void addMockResults(String sql, ResultSet resultSet) {
        mockResultSests.put(sql, resultSet);
    }
}
