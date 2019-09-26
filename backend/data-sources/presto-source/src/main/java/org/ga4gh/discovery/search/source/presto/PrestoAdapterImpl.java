package org.ga4gh.discovery.search.source.presto;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class PrestoAdapterImpl implements PrestoAdapter {

    private final String url;
    private final String username;
    private final ServiceAccountAuthenticator authenticator;

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
        query(prestoSQL, resultProcessor, true);
    }

    private void query(String prestoSQL, Consumer<ResultSet> resultProcessor, boolean shouldRetryOnAuthFailure) {
        try (Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            resultProcessor.accept(stmt.executeQuery(prestoSQL));
        } catch (SQLException e) {
            if (shouldRetryOnAuthFailure && isAuthenticationFailure(e)) {
                log.trace("Encountered SQLException is recoverable. Renewing authentication credentials and retrying query");
                authenticator.refreshAccessToken();
                query(prestoSQL, resultProcessor, false);
            } else {
                log.trace("Encountered SQLException is not recoverable, failing query");
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void query(String prestoSQL, List<Object> params, Consumer<ResultSet> resultProcessor) {
        query(prestoSQL, params, resultProcessor);
    }

    public void query(String prestoSQL, List<Object> params, Consumer<ResultSet> resultProcessor, boolean shouldRetryOnAuthFailure) {
        try (Connection connection = getConnection()) {
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
            if (shouldRetryOnAuthFailure && isAuthenticationFailure(e)) {
                log.trace("Encountered SQLException is recoverable. Renewing authentication credentials and retrying query");
                authenticator.refreshAccessToken();
                query(prestoSQL, params, resultProcessor, false);
            } else {
                log.trace("Encountered SQLException is not recoverable, failing query");
                throw new RuntimeException(e);
            }
        }
    }

    private Connection getConnection() throws SQLException {
        log.trace("Establishing connection to presto server: {}", url);
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("SSL", "true");
        connectionProperties.setProperty("user", username);
        connectionProperties.setProperty("accessToken", authenticator.getAccessToken());
        Connection connection = DriverManager.getConnection(url, connectionProperties);
        log.trace("Successfully established connection to presto server: {}", url);
        return connection;

    }

    private boolean isAuthenticationFailure(SQLException e) {
        Throwable cause = e.getCause();
        String message;
        if (cause != null) {
            message = cause.getMessage();
        } else {
            message = e.getMessage();
        }

        if (message != null) {
            Pattern errorResponsePattern = Pattern
                .compile(".*(?<jsonResponse>JsonResponse\\{.*(?<statusCode>statusCode=[0-9]{3}).*}).*");
            Matcher matcher = errorResponsePattern.matcher(message);
            if (matcher.find()) {
                int statusCode = Integer.parseInt(matcher.group("statusCode").split("=")[1]);

                if (statusCode == 403) {
                    throw new ServiceAccountAuthenticationException(
                        "Application is not authorized to access presto deployment " + url
                            + ". Please verify sa credentials and presto config. " + matcher.group("jsonResponse"));
                }

                return statusCode == 401;
            }
        }
        return false;
    }


}
