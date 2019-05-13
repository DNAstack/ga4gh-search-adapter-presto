package org.ga4gh.discovery.search.source.presto;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.request.SearchRequest;
import org.ga4gh.discovery.search.result.ResultRow;
import org.ga4gh.discovery.search.result.ResultValue;
import org.ga4gh.discovery.search.result.SearchResult;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.collect.ImmutableList;

import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Query;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrestoSearchSource implements SearchSource {

    @Value("${presto.results.limit.max}")
    private int maxResultsLimit;

    private final PrestoAdapter prestoAdapter;
    private final Metadata metadata;

    public PrestoSearchSource(PrestoAdapter prestoAdapter) {
        this.prestoAdapter = prestoAdapter;
        this.metadata = new Metadata(new PrestoMetadata(prestoAdapter));
    }

    @Override
    public List<Table> getTables() {
        return metadata.getTables();
    }

    @Override
    public List<Field> getFields(String tableName) {
        List<Table> tableList =
                tableName == null ? getTables() : ImmutableList.of(metadata.getTable(tableName));
        ImmutableList.Builder<Field> listBuilder = ImmutableList.builder();
        for (Table table : tableList) {
            listBuilder.addAll(metadata.getTableMetadata(table.getName()).getFields());
        }
        return listBuilder.build();
    }

    private Query getQuery(SearchRequest query) {
        if (query.getSqlQuery() != null) {
            if (query.getJsonQuery() != null) {
                log.warn("Received both JSON and SQL query, ignoring JSON");
            }
            return parseQuery(query.getSqlQuery());
        } else if (query.getJsonQuery() != null) {
            log.debug("Processing JSON query");
            return query.getJsonQuery();
        } else {
            throw new IllegalArgumentException(
                    "Either JSON or SQL query has to be present in search request");
        }
    }

    private Query parseQuery(String sql) {
        log.debug("Processing SQL query: {}", sql);
        return (Query) new SqlParser().createStatement(sql, new ParsingOptions());
    }

    @Override
    public SearchResult search(SearchRequest searchRequest) {
        Query query = getQuery(searchRequest);
        QueryContext queryContext = new QueryContext(query, metadata);
        SearchQueryTransformer queryTransformer =
                new SearchQueryTransformer(metadata, query, queryContext);
        String prestoSqlString = queryTransformer.toPrestoSQL();
        log.info("Transformed to SQL: {}", prestoSqlString);
        List<Field> fields = new ArrayList<>();
        List<ResultRow> results = new ArrayList<>();

        prestoAdapter.query(
                prestoSqlString,
                rs -> {
                    try {
                        fields.addAll(queryTransformer.validateAndGetFields(rs.getMetaData()));

                        while (rs.next()) {
                            results.add(extractRow(rs, fields));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        SearchResult searchResult = new SearchResult(fields, results);
        return searchResult;
    }

    private ResultRow extractRow(ResultSet rs, List<Field> fields) throws SQLException {
        List<ResultValue> values = new ArrayList<>();

        for (int i = 1; i <= fields.size(); i++) {
            values.add(new ResultValue(fields.get(i - 1), rs.getString(i)));
        }

        return new ResultRow(values);
    }
}
