package org.ga4gh.discovery.search.source.presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PrestoAdapterImpl implements PrestoAdapter {

    private final String url;
    private final String username;
    private final String password;

    @Override
    public PrestoTableMetadata getMetadata(PrestoTable table) {
        ImmutableList.Builder<PrestoField> listBuilder = ImmutableList.<PrestoField>builder();
        query(
                "show columns from " + table.getQualifiedName(),
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
        try {
            Connection connection = getConnection();
            Statement stmt = connection.createStatement();
            resultProcessor.accept(stmt.executeQuery(prestoSQL));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
}
