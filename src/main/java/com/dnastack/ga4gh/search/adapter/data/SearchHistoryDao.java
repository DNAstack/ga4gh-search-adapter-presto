package com.dnastack.ga4gh.search.adapter.data;


import com.dnastack.ga4gh.search.adapter.model.SearchRequest;
import java.util.List;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

public interface SearchHistoryDao {

    @Transaction
    @SqlUpdate("INSERT INTO search_history(user_id,submission_date,sql_query,succeeded) VALUES(:userId, :submissionDate, :sqlQuery,:succeeded)")
    void saveSearch(@BindBean SearchHistory searchRequest);

    @RegisterRowMapper(SearchHistoryRowMapper.class)
    @SqlQuery("SELECT * FROM search_history WHERE user_id = :userId ORDER BY submission_date DESC OFFSET :offset LIMIT :limit ")
    List<SearchHistory> getSearchHistories(@Bind("userId") String userId,@Bind("offset") int offset,@Bind("limit") int limit);

    @SqlQuery("SELECT COUNT(*) FROM search_history WHERE user_id = :userId")
    int countUsersSearchRequest(@Bind("userId") String userId);

}
