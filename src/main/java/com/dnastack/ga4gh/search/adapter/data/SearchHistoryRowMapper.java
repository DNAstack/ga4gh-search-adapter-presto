package com.dnastack.ga4gh.search.adapter.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public class SearchHistoryRowMapper implements RowMapper<SearchHistory> {

    @Override
    public SearchHistory map(ResultSet rs, StatementContext ctx) throws SQLException {
        SearchHistory request = new SearchHistory();
        request.setUserId(rs.getString("user_id"));
        request.setSubmissionDate(rs.getTimestamp("submission_date").toLocalDateTime().atZone(ZoneId.of("UTC")));
        request.setSqlQuery(rs.getString("sql_query"));
        request.setSucceeded(rs.getBoolean("succeeded"));
        return request;
    }
}
