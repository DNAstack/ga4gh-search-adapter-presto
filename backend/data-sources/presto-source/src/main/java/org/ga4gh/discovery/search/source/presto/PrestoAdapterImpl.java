package org.ga4gh.discovery.search.source.presto;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.ExpressionFormatter;
import lombok.AllArgsConstructor;

import java.sql.*;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@AllArgsConstructor
public class PrestoAdapterImpl implements PrestoAdapter {

    private final String url;
    private final String username;
    private final String password;

    @Override
    public PrestoTableMetadata getMetadata(PrestoTable table) {
        ImmutableList.Builder<PrestoField> listBuilder = ImmutableList.<PrestoField>builder();
        query(
                "show columns from "
                        + ExpressionFormatter.formatQualifiedName(table.getQualifiedName()),
                resultSet -> {
                    try {
                        while (resultSet.next()) {
                            listBuilder.add(
                                    new PrestoField(
                                            resultSet.getString(1), resultSet.getString(2)));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(
                                "Error while retrieving data from result set", e);
                    }
                });
        return new PrestoTableMetadata(table, listBuilder.build());
    }

    @Override
    public void query(String prestoSQL, Consumer<ResultSet> resultProcessor) {
        query(prestoSQL, Optional.empty(), resultProcessor);
    }

    @Override
    public void query(String prestoSQL, Optional<List<Object>> params, Consumer<ResultSet> resultProcessor) {
        try {
            Connection connection = getConnection();
            PreparedStatement stmt = connection.prepareStatement(prestoSQL);
            if (params.isPresent()) {
                int i = 1;
                for (Object p : params.get()) {
                    //TODO: Null guard
                    stmt.setString(i, p.toString());
                    i++;

                }
            }
            resultProcessor.accept(stmt.executeQuery());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
}
