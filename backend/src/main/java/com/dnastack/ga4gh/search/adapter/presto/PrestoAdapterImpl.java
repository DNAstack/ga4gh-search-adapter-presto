package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.auth.ServiceAccountAuthenticator;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

@Slf4j
public class PrestoAdapterImpl implements PrestoAdapter {

    private String prestoDatasourceUrl;
    private final ServiceAccountAuthenticator authenticator;
    private static final String DEFAULT_PRESTO_USER_NAME = "ga4gh-search-adapter-presto";
    private static final int DEFAULT_PAGE_SIZE = 100;

    public PrestoAdapterImpl(String prestoDatasourceUrl, ServiceAccountAuthenticator accountAuthenticator) {
        this.prestoDatasourceUrl = prestoDatasourceUrl;
        this.authenticator = accountAuthenticator;
    }

    @Override
    public PrestoTableMetadata getMetadata(PrestoTable table) {
        ImmutableList.Builder<PrestoField> listBuilder = ImmutableList.<PrestoField>builder();
        String query = "show columns from" + table.getQualifiedName();
        PagingResultSetConsumer resultSetConsumer = query(query, DEFAULT_PAGE_SIZE);
        resultSetConsumer.consumeAll(
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
    public PagingResultSetConsumer query(String prestoSQL) {
        return query(prestoSQL, DEFAULT_PAGE_SIZE);
    }

    @Override
    public PagingResultSetConsumer query(String prestoSQL, Integer pageSize) {
        return query(prestoSQL, pageSize, true);
    }

    public PagingResultSetConsumer query(String prestoSQL, Integer pageSize, boolean shouldRetryOnAuthFailure) {
        try (Connection connection = getConnection()) {
            pageSize = pageSize == null ? DEFAULT_PAGE_SIZE : pageSize;
            Statement stmt = connection.createStatement();
            return new PagingResultSetConsumer(stmt.executeQuery(prestoSQL), pageSize);
        } catch (SQLException e) {
            if (shouldRetryOnAuthFailure && isAuthenticationFailure(e)) {
                log.trace("Encountered SQLException is recoverable. Renewing authentication credentials and retrying query");
                authenticator.refreshAccessToken();
                return query(prestoSQL, pageSize, false);
            } else {
                log.trace("Encountered SQLException is not recoverable, failing query");
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public PagingResultSetConsumer query(String prestoSQL, List<Object> params) {
        return query(prestoSQL, params, DEFAULT_PAGE_SIZE);
    }


    @Override
    public PagingResultSetConsumer query(String prestoSQL, List<Object> params, Integer pageSize) {
        return query(prestoSQL, params, pageSize);
    }

    public PagingResultSetConsumer query(String prestoSQL, List<Object> params, Integer pageSize, boolean shouldRetryOnAuthFailure) {
        try (Connection connection = getConnection()) {
            pageSize = pageSize == null ? DEFAULT_PAGE_SIZE : pageSize;
            PreparedStatement stmt = connection.prepareStatement(prestoSQL);
            if (params != null) {
                int i = 1;
                for (Object p : params) {
                    //TODO: Could NPE here
                    stmt.setString(i, p.toString());
                    i++;
                }
            }
            return new PagingResultSetConsumer(stmt.executeQuery(), pageSize);
        } catch (SQLException e) {
            if (shouldRetryOnAuthFailure && isAuthenticationFailure(e)) {
                log.trace("Encountered SQLException is recoverable. Renewing authentication credentials and retrying query");
                authenticator.refreshAccessToken();
                return query(prestoSQL, params, pageSize, false);
            } else {
                log.trace("Encountered SQLException is not recoverable, failing query");
                throw new RuntimeException(e);
            }
        }
    }

    private Connection getConnection() throws SQLException {
        log.trace("Establishing connection to presto server: {}", prestoDatasourceUrl);
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("SSL", "true");
        connectionProperties.setProperty("user", getUserNameForPrestoRequest());
        connectionProperties.setProperty("accessToken", authenticator.getAccessToken());
        Connection connection = DriverManager.getConnection(prestoDatasourceUrl, connectionProperties);
        log.trace("Successfully established connection to presto server: {}", prestoDatasourceUrl);
        return connection;

    }

    /**
     * If the Incoming request has authenticaiton information, use the attached user principal as the username to pass
     * to presto, otherwise, set  the a deafault username
     */
    private String getUserNameForPrestoRequest() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.isAuthenticated()) {
            return authentication.getPrincipal().toString();
        } else {
            return DEFAULT_PRESTO_USER_NAME;
        }
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
                return statusCode == 401 || statusCode == 403;
            }
        }
        return false;
    }


}
