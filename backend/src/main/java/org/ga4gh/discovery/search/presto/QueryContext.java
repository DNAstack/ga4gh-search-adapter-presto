package org.ga4gh.discovery.search.presto;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ga4gh.discovery.search.model.Field;

import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;

public class QueryContext {

    private final Query query;
    private final Metadata metadata;
    private final Map<String, TableMetadata> fromTables = new HashMap<String, TableMetadata>();
    private final List<ResolvedColumn> selectColumns = new ArrayList<>();

    public QueryContext(Query query, Metadata metadata) {
        this.query = query;
        this.metadata = metadata;
        init();
    }

    private void init() {
        QuerySpecification querySpec = (QuerySpecification) query.getQueryBody();
        checkArgument(querySpec.getFrom().isPresent(), "missing FROM clause");
        new FromTablesCollector(metadata, fromTables).process(querySpec.getFrom().get());
        new SelectColumnsCollector(fromTables, selectColumns).process(querySpec.getSelect());
    }

    public int getSelectColumnCount() {
        return selectColumns.size();
    }

    public ResolvedColumn getSelectColumn(int i) {
        return selectColumns.get(i);
    }

    public List<Field> getSelectFields() {
        return selectColumns.stream().map(col -> col.getResolvedField()).collect(toList());
    }

    public boolean hasFromTable(String tableName) {
        return fromTables.containsKey(tableName);
    }
}
