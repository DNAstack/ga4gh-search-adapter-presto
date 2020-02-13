package com.dnastack.ga4gh.search.adapter.presto;

import com.dnastack.ga4gh.search.adapter.monitoring.Monitor;
import com.dnastack.ga4gh.search.adapter.security.ServiceAccountAuthenticator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
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
import org.springframework.security.core.userdetails.User;
import org.springframework.security.oauth2.jwt.Jwt;

@Slf4j
public class PrestoAdapter {

    private String prestoDatasourceUrl;
    private final ServiceAccountAuthenticator authenticator;
    private static final String DEFAULT_PRESTO_USER_NAME = "ga4gh-search-adapter-presto";
    private static final int DEFAULT_PAGE_SIZE = 100;
    private final Counter queryCounter;
    private final Timer latencyTime;

    public PrestoAdapter(String prestoDatasourceUrl, ServiceAccountAuthenticator accountAuthenticator, Monitor monitor) {
        this.prestoDatasourceUrl = prestoDatasourceUrl;
        this.authenticator = accountAuthenticator;
        this.queryCounter = monitor.registerCounter("search.queries.queries_performed",
            "The raw number of queries performed over a given step of time.");
        this.latencyTime = monitor.registerRequestTimer("search.queries.query_latency",
            "The average latency of queries performed over a given step of time.");
    }

    public PagingResultSetConsumer query(String prestoSQL) {
        return query(prestoSQL, DEFAULT_PAGE_SIZE);
    }

    public PagingResultSetConsumer query(String prestoSQL, Integer pageSize) {
        return query(prestoSQL, pageSize, true);
    }

    public PagingResultSetConsumer query(String prestoSQL, Integer pageSize, boolean shouldRetryOnAuthFailure) {
        this.queryCounter.increment();
        int finalPageSize = pageSize == null ? DEFAULT_PAGE_SIZE : pageSize;
        return latencyTime.record(() -> {
            try (Connection connection = getConnection()) {
                Statement stmt = connection.createStatement();
                return new PagingResultSetConsumer(stmt.executeQuery(prestoSQL), finalPageSize);
            } catch (SQLException e) {
                if (shouldRetryOnAuthFailure && isAuthenticationFailure(e)) {
                    log.trace("Encountered SQLException is recoverable. Renewing authentication credentials and retrying query");
                    authenticator.refreshAccessToken();
                    return query(prestoSQL, finalPageSize, false);
                } else {
                    log.trace("Encountered SQLException is not recoverable, failing query");
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public PagingResultSetConsumer query(String prestoSQL, List<Object> params) {
        return query(prestoSQL, params, DEFAULT_PAGE_SIZE);
    }


    public PagingResultSetConsumer query(String prestoSQL, List<Object> params, Integer pageSize) {
        return query(prestoSQL, params, pageSize);
    }

    public PagingResultSetConsumer query(String prestoSQL, List<Object> params, Integer pageSize, boolean shouldRetryOnAuthFailure) {
        int finalPageSize = pageSize == null ? DEFAULT_PAGE_SIZE : pageSize;
        return latencyTime.record(() -> {
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
                return new PagingResultSetConsumer(stmt.executeQuery(), pageSize);
            } catch (SQLException e) {
                if (shouldRetryOnAuthFailure && isAuthenticationFailure(e) && authenticator.requiresAuthentication()) {
                    log.trace("Encountered SQLException is recoverable. Renewing authentication credentials and retrying query");
                    authenticator.refreshAccessToken();
                    return query(prestoSQL, params, pageSize, false);
                } else {
                    log.trace("Encountered SQLException is not recoverable, failing query");
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private Connection getConnection() throws SQLException {
        log.trace("Establishing connection to presto server: {}", prestoDatasourceUrl);
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("SSL", "true");
        connectionProperties.setProperty("user", getUserNameForPrestoRequest());

        if (authenticator.requiresAuthentication()) {
            connectionProperties.setProperty("accessToken", authenticator.getAccessToken());
        }

        Connection connection = DriverManager.getConnection(prestoDatasourceUrl, connectionProperties);
        log.trace("Successfully established connection to presto server: {}", prestoDatasourceUrl);
        return connection;

    }

    /**
     * If the Incoming request has authentication information, use the attached user principal as the username to pass
     * to presto, otherwise, set  the a default username
     */
    private String getUserNameForPrestoRequest() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.isAuthenticated()) {
            Object principal = authentication.getPrincipal();
            if (principal instanceof User) {
                return ((User) principal).getUsername();
            } else if (principal instanceof Jwt) {
                return ((Jwt) principal).getSubject();
            } else {
                return principal.toString();
            }
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
