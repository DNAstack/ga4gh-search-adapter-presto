package org.ga4gh.discovery.search.source.presto;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.query.SearchQuery;
import org.ga4gh.discovery.search.result.ResultRow;
import org.ga4gh.discovery.search.result.ResultValue;
import org.ga4gh.discovery.search.result.SearchResult;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.beans.factory.annotation.Value;
import com.google.common.collect.ImmutableList;
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

    @Override
    public SearchResult search(SearchQuery query) {
        log.info("Received query: {}", query);
        QueryContext queryContext = new QueryContext(query, metadata);
        SearchQueryTransformer queryTransformer =
                new SearchQueryTransformer(metadata, query, queryContext);
        String prestoSqlString = queryTransformer.toPrestoSQL();
        log.info("Transformed to SQL: {}", prestoSqlString);
        List<ResultRow> results = new ArrayList<>();
        List<Field> resultFields = new ArrayList<>();

        prestoAdapter.query(
                prestoSqlString,
                rs -> {
                    try {
                        List<Field> fields =
                                queryTransformer.validateAndGetFields(rs.getMetaData());

                        while (rs.next()) {
                            List<ResultValue> values = new ArrayList<>();

                            for (int i = 1; i <= fields.size(); i++) {
                                values.add(new ResultValue(fields.get(i - 1), rs.getString(i)));
                            }

                            results.add(new ResultRow(values));
                        }
                        resultFields.addAll(fields);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        SearchResult searchResult = new SearchResult(resultFields, results);
        return searchResult;
    }
}
