package org.ga4gh.discovery.search.source.presto;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Query;
import lombok.extern.slf4j.Slf4j;
import org.ga4gh.discovery.search.Field;
import org.ga4gh.discovery.search.Table;
import org.ga4gh.discovery.search.Type;
import org.ga4gh.discovery.search.request.SearchRequest;
import org.ga4gh.discovery.search.result.ResultRow;
import org.ga4gh.discovery.search.result.ResultValue;
import org.ga4gh.discovery.search.result.SearchResult;
import org.ga4gh.discovery.search.source.SearchSource;
import org.springframework.beans.factory.annotation.Value;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class PrestoSearchSource implements SearchSource {

    @Value("${presto.results.limit.max}")
    private int maxResultsLimit;

    private final PrestoAdapter prestoAdapter;
    private final Metadata metadata;
    private Map<String, PrestoTable> tablesWhitelist;

    public PrestoSearchSource(PrestoAdapter prestoAdapter) {
        this.prestoAdapter = prestoAdapter;
        this.metadata = new Metadata(new PrestoMetadata(prestoAdapter));
        //this.metadata = new Metadata(new PrestoMetadata(getTablesWhitelist()));
    }

    @Override
    public List<Table> getTables() {
        return metadata.getTables();
    }

    //TODO: Destroy this
    private synchronized Map<String, PrestoTable> getTablesWhitelist() {
        if (tablesWhitelist == null) {
            tablesWhitelist = populateTables2();
        }
        return tablesWhitelist;
    }

    private Map<String, String> populateTables() {
        Map<String, List<Table>> datasets = getDatasets();
        Map<String, String> tables = new HashMap<>();
        datasets.forEach((dataset, tableList) -> tableList.forEach(table -> {
            tables.put(String.format("%s.%s.%s", dataset, table.getSchema(), table.getName()),
                    String.format("\"%s\".\"%s\".\"%s\"", dataset, table.getSchema(), table.getName()));
        }));
        return tables;
    }

    private Map<String, PrestoTable> populateTables2() {
        Map<String, PrestoTable> tables = new HashMap<>();
        List<PrestoCatalog> catalogs = getCatalogs();
        for (PrestoCatalog catalog : catalogs) {
            //TODO: Better way of filtering
            //TODO: Also filter out information schemas.
            //Don't show system tables.
            if (catalog.getName().equals("system")) {
                continue;
            }
            List<ResultRow> catalogTables = getTables(catalog);
            for (ResultRow table : catalogTables) {
                String tableName = table.getValues().get(0).getValue().toString();
                String tableSchema = table.getValues().get(1).getValue().toString();
                String id = String.format("%s.%s.%s", catalog.getName(), tableSchema, tableName);
                tables.put(id, new PrestoTable(tableName, tableSchema, catalog.getName(), tableSchema, tableName));
            }
        }
        return tables;
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
    public SearchResult getDataset(String id) {
        if (!getTablesWhitelist().containsKey(id)) {
            return null;
        }
        PrestoTable table = getTablesWhitelist().get(id);
        SearchRequest request = new SearchRequest(null, String.format("SELECT * FROM %s", table.getQualifiedName()));
        SearchResult result = search(request);
        return result;
    }

    @Override
    public Map<String, List<Table>> getDatasets() {
        //TODO: Revaluate where this lives
        List<PrestoCatalog> catalogs = getCatalogs();
        Map<String, List<Table>> tables = new HashMap<>();
        for (PrestoCatalog catalog : catalogs) {
            if (catalog.getName().equals("system")) {
                continue;
            }
            List<ResultRow> catalogTables = getTables(catalog);
            tables.put(catalog.getName(), catalogTables.stream().map(table ->
                    new Table(table.getValues().get(0).getValue().toString(), table.getValues().get(1).getValue().toString())).collect(Collectors.toList()));
        }
        return tables;
    }

    private List<ResultRow> getTables(PrestoCatalog catalog) {
        String sql = String.format("select table_name, table_schema from \"%s\".information_schema.tables", catalog.getName());
//        List<Object> params = new ArrayList<>();
//        params.add(catalog.GetName());
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("table_name", "Table Name", Type.STRING, null, null, null));
        fields.add(new Field("table_schema", "Table Schema", Type.STRING, null, null, null));
        return query(sql, Optional.empty(), fields);
    }

    private List<PrestoCatalog> getCatalogs() {
        String sql = "show catalogs";
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("Catalog", "Catalog", Type.STRING, null, null, null));
        List<ResultRow> results = query(sql, fields);
        return results.stream().map(row -> new
                PrestoCatalog(row.getValues().get(0).getValue().toString(), new ArrayList<>())).collect(Collectors.toList());

    }

    //TODO: Better string substitution
    private List<ResultRow> getSchemas(PrestoCatalog catalog) {
        String sql = String.format("SHOW SCHEMAS FROM \"%s\"", catalog.getName());
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("Schema", "Schema", Type.STRING, null, null, null));
        return query(sql, fields);
    }

    private List<ResultRow> getTables(String schema) {
        String sql = String.format("SHOW TABLES FROM \"%s\"", schema);
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("Table", "Table", Type.STRING, null, null, null));
        return query(sql, fields);
    }

    private List<ResultRow> query(String sql, List<Field> fields) {
        return query(sql, Optional.empty(), fields);
    }

    private List<ResultRow> query(String sql, Optional<List<Object>> params, List<Field> fields) {
        List<ResultRow> results = new ArrayList<>();
        prestoAdapter.query(
                sql,
                params,
                rs -> {
                    try {
                        while (rs.next()) {
                            results.add(extractRow(rs, fields));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
        return results;
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
