package org.ga4gh.discovery.search.source.presto;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Query;
import lombok.extern.slf4j.Slf4j;
import org.ga4gh.dataset.SchemaId;
import org.ga4gh.dataset.model.Dataset;
import org.ga4gh.dataset.model.Schema;
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

    public PrestoSearchSource(PrestoAdapter prestoAdapter) {
        this.prestoAdapter = prestoAdapter;
        this.metadata = new Metadata(buildMetadata());
    }

    @Override
    public List<Table> getTables() {
        return metadata.getTables();
    }

    private PrestoMetadata buildMetadata() {
        //TODO: these object structures are unusual
        // AND GROSS!!!
        List<PrestoCatalog> emptyCatalogs = getCatalogs();
        List<PrestoCatalog> populatedCatalogs = new ArrayList<>(emptyCatalogs.size());
        Map<String, PrestoTable> tables = new HashMap<>();
        for (PrestoCatalog catalog : emptyCatalogs) {
            populatedCatalogs.add(populateSchemas(catalog));
            tables.putAll(getTables(catalog));
        }
        return new PrestoMetadata(prestoAdapter, populatedCatalogs, tables);
    }


    @Override
    public List<Field> getFields(String tableName) {
        List<Table> tableList =
                tableName == null ? getTables() : ImmutableList.of(metadata.getTable(tableName));
        ImmutableList.Builder<Field> listBuilder = ImmutableList.builder();
        for (Table table : tableList) {
            if (tableName == null) {
                listBuilder.addAll(metadata.getTableMetadata(table.getName()).getFields());
            } else {
                //TODO: breaky breaky, hacky hacky
                listBuilder.addAll(metadata.getTableMetadata(tableName).getFields());
            }
        }
        return listBuilder.build();
    }

    @Override
    public Dataset getDataset(String id) {
        SearchResult tableData = getEntireTable(id);
        if (tableData == null) {
            return null;
        }

        //TODO: List<Object> vs List<Map<String, Object>>
        List<Object> results = new ArrayList<>(tableData.getResults().size());
        for (ResultRow row : tableData.getResults()) {
            Map<String, Object> rowData = new HashMap<>();
            for (ResultValue value : row.getValues()) {
                rowData.put(value.getField().getId(), value.getValue());
            }
            results.add(rowData);
        }
        //TODO: fix
        SchemaId schemaId = SchemaId.of("ga4gh.example.com");
        Schema schema = new Schema(schemaId, null);
        return new Dataset(schema, Collections.unmodifiableList(results), null);
    }

    public SearchResult getEntireTable(String tableQualifiedName) {
        if (!metadata.hasTable(tableQualifiedName)) {
            log.info("Got an invalid table name: {}", tableQualifiedName);
            return null;
        }
        PrestoTable table = metadata.getPrestoTable(tableQualifiedName);
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        List<Field> fields = getFields(tableQualifiedName);
        //List<Field> fields = getFields(table.getQualifiedName().toString());
        for (Field f : fields) {
            sqlBuilder.append(String.format(" %s,", f.getName()));
        }
        sqlBuilder.deleteCharAt(sqlBuilder.lastIndexOf(","));
        sqlBuilder.append(String.format(" FROM %s", table.getQualifiedName()));
        SearchRequest request = new SearchRequest(null, sqlBuilder.toString());
        //SearchRequest request = new SearchRequest(null, String.format("SELECT * FROM %s", table.getQualifiedName()));
        SearchResult result = search(request);
        return result;
    }

    @Override
    public Map<String, List<Table>> getDatasets() {
        return null;
        //TODO: Revaluate where this lives
//        List<PrestoCatalog> catalogs = getCatalogs();
//        Map<String, List<Table>> tables = new HashMap<>();
//        for (PrestoCatalog catalog : catalogs) {
////            if (catalog.getName().equals("system")) {
////                continue;
////            }
//            List<ResultRow> catalogTables = getTables(catalog);
//            tables.put(catalog.getName(), catalogTables.stream()
//                    //TODO: Better filtering, remove information schemas
//                    .filter(table -> !table.getValues().get(1).getValue().toString().equals("information_schema"))
//                    .map(table ->
//                    new Table(table.getValues().get(0).getValue().toString(), table.getValues().get(1).getValue().toString())).collect(Collectors.toList()));
//        }
//        return tables;
    }

    private Map<String, PrestoTable> getTables(PrestoCatalog catalog) {
        Map<String, PrestoTable> tables = new HashMap<>();
        String sql = String.format("select table_name, table_schema from \"%s\".information_schema.tables", catalog.getName());
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("table_name", "Table Name", Type.STRING, null, null, null));
        fields.add(new Field("table_schema", "Table Schema", Type.STRING, null, null, null));
        List<ResultRow> catalogTables = query(sql, fields);
        for (ResultRow table : catalogTables) {
            //TODO: Better string manip
            String tableName = table.getValues().get(0).getValue().toString();
            String tableSchema = table.getValues().get(1).getValue().toString();
            String escapedName = String.format("\"%s\"", tableName);
            String escapedSchema = String.format("\"%s\"", tableSchema);
            String id = String.format("%s.%s.%s", catalog.getName(), tableSchema, tableName);
            tables.put(id, new PrestoTable(escapedName, escapedSchema, String.format("\"%s\"", catalog.getName())/* catalog.getName()*/, escapedSchema, escapedName));
//                tables.put(id, new PrestoTable(escapedName, escapedSchema, catalog.getName(), tableSchema, tableName));
        }

        return tables;
    }

    private List<PrestoCatalog> getCatalogs() {
        final String sql = "show catalogs";
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("Catalog", "Catalog", Type.STRING, null, null, null));
        List<ResultRow> results = query(sql, fields);
        return results.stream().map(row -> new
                //TODO: SCHEMAS SHOULD BE POPULATED HERE
                PrestoCatalog(row.getValues().get(0).getValue().toString(), new ArrayList<>()))
                //TODO: Check blacklist here
                .filter(prestoCatalog -> !prestoCatalog.getName().equals("system"))
                .collect(Collectors.toList());

    }

    private PrestoCatalog populateSchemas(PrestoCatalog catalog) {
        String sql = String.format("SHOW SCHEMAS FROM \"%s\"", catalog.getName());
        List<Field> fields = Collections.singletonList(new Field("Schema", "Schema", Type.STRING, null, null, null));
        List<ResultRow> results = query(sql, fields);

        List<PrestoSchema> schemas = new ArrayList<>();
        for (ResultRow row : results) {
            List<ResultValue> values = row.getValues();
            values.stream()
                    .filter(v -> !v.getValue().toString().equals("information"))
                    .forEach(v -> schemas.add(new PrestoSchema(v.getValue().toString())));
        }

        return new PrestoCatalog(catalog.getName(), schemas);
    }

//    private List<PrestoSchema> getSchemas(PrestoCatalog catalog) {
//        String sql = String.format("SHOW SCHEMAS FROM \"%s\"", catalog.getName());
//        List<Field> fields = Arrays.asList(new Field("Schema", "Schema", Type.STRING, null, null, null));
//        List<ResultRow> results = query(sql, fields);
//
//        List<PrestoSchema> schemas = new ArrayList<>();
//        for (ResultRow row : results) {
//           List<ResultValue> values = row.getValues();
//           for (ResultValue value : values) {
//               String name = value.getValue().toString();
//               schemas.add(new PrestoSchema(name));
//           }
//        }
//
//        return schemas;
//    }


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
        //TODO: After this line, we have a QUERY obj which should _already be valid_.
        // Thus, does it make sense doing the escaping, etc. down the pipeline?
        // Alternatively, does this mean query generation should exist at a different layer?
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
