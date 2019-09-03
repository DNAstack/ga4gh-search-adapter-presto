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
                    + table.getQualifiedName(),
                //TODO: PUT BACK
//                        + ExpressionFormatter.formatQualifiedName(table.getQualifiedName()),
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

    @Override
    public void query(String prestoSQL, List<Object> params, Consumer<ResultSet> resultProcessor) {
        try {
            Connection connection = getConnection();
            PreparedStatement stmt = connection.prepareStatement(prestoSQL);
            if (params != null) {
                int i = 1;
                for (Object p : params) {
                    //TODO: Could NPE here
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
